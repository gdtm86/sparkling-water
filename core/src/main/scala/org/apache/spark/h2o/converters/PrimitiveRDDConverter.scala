/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.h2o.converters

import org.apache.spark.h2o.H2OContextUtils.NodeDesc
import org.apache.spark.h2o._
import org.apache.spark.{Logging, SparkContext, TaskContext}
import water.Key
import water.fvec.H2OFrame

import scala.collection.immutable
import scala.language.implicitConversions
import scala.reflect.runtime.universe._

private[converters] object PrimitiveRDDConverter extends Logging with ConvertersUtils{

  def toH2OFrame[T: TypeTag](sc: SparkContext, rdd: RDD[T], frameKeyName: Option[String]): H2OFrame = {
    import org.apache.spark.h2o.H2OTypeUtils._
    import org.apache.spark.h2o.ReflectionUtils._

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

    val fr = getFrameOrNone(keyName)
    if(fr.isDefined){
      // return early if this frame already exist
      return fr.get
    }

    val fnames = Array[String]("values")
    val ftypes = Array[Class[_]](typ(typeOf[T]))
    val vecTypes = ftypes.indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray

    convert[T](sc, rdd, keyName, fnames, vecTypes, perPrimitiveRDDPartition())
  }


  /**
    *
    * @param keyName key of the frame
    * @param vecTypes h2o vec types
    * @param uploadPlan plan which assigns each partition h2o node where the data from that partition will be uploaded
    * @param context spark task context
    * @param it iterator over data in the partition
    * @tparam T type of data inside the RDD
    * @return pair (partition ID, number of rows in this partition)
    */
  private[this]
  def perPrimitiveRDDPartition[T]() // extra arguments for this transformation
                                 (keyName: String, vecTypes: Array[Byte], uploadPlan: immutable.Map[Int, NodeDesc]) // general arguments
                                 (context: TaskContext, it: Iterator[T]): (Int, Long) = { // arguments and return types needed for spark's runJob input
    val conn = new DataUploadHelper.ConnectionHolder(uploadPlan(context.partitionId()))
    // Creates array of H2O NewChunks; A place to record all the data in this partition
    conn.createChunksRemotely(keyName, vecTypes, context.partitionId())
    // try to wait for reply to ensure we can continue with sending
    it.foreach { r =>
      r match {
        case n: Number => conn.put(0, n)
        case n: Boolean => conn.put(0, n)
        case n: String => conn.put(0, n)
        case n: java.sql.Timestamp => conn.put(0, n)
        case _ => conn.putNA(0)
      }
      conn.increaseRowCounter()
    }
    //Compress & write data in partitions to H2O Chunks
    conn.closeChunksRemotely()
    conn.close()

    // Return Partition number and number of rows in this partition
    (context.partitionId, conn.numOfRows)
  }

}
