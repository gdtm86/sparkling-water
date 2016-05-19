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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.h2o.utils.H2OSchemaUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, TaskContext}
import water.fvec.H2OFrame

/**
* H2O H2OFrame wrapper providing RDD[Row]=DataFrame API.
*
* @param hc H2O Context
* @param frame frame which will be wrapped as DataFrame
*/
private[converters]
class H2ODataFrame(@transient val hc: H2OContext,
                   @transient val frame: H2OFrame)
  extends RDD[InternalRow](hc.sparkContext, Nil) {

  // Fields outside the compute method are accessed in the driver node and therefore also by H2O node in client mode on
  // that node
  /** Cache frame key to get H2OFrame from the K/V store */
  val keyName = frame._key.toString
  val numCols = frame.numCols()
  val externalBackend = hc.getConf.runsInExternalClusterMode
  // This is small computation done at the moment of
  // creation H2ODataFrame ( not lazy ), but it gives us benefit of not having to compute types for each partition
  val types = frame.vecs().map( v => vecTypeToDataType(v))
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val con = ConverterUtils.getReadConverterContext(externalBackend, keyName, split.index)

    new Iterator[InternalRow] {
      /** Mutable reusable row returned by iterator */
      val mutableRow = new GenericMutableRow(numCols)

      def hasNext: Boolean = con.hasNext

      override def next(): InternalRow = {
        (0 until numCols).foreach { i =>
          if (con.isNA(i)) {
            mutableRow.setNullAt(i)
          } else {
            types(i) match {
              case ByteType => mutableRow.setByte(i, con.getByte(i))
              case ShortType => mutableRow.setShort(i, con.getShort(i))
              case IntegerType => mutableRow.setInt(i, con.getInt(i))
              case LongType => mutableRow.setLong(i, con.getLong(i))
              case FloatType => mutableRow.setFloat(i, con.getFloat(i))
              case DoubleType => mutableRow.setDouble(i, con.getDouble(i))
              case BooleanType => mutableRow.setBoolean(i, con.getBoolean(i))
              case StringType => mutableRow.update(i, con.getUTF8String(i))
              case TimestampType => mutableRow.setLong(i, con.getTimestamp(i))
              case _ => ???
            }
          }
        }
        con.increaseRowIdx()
        // Return result
        mutableRow
      }
    }

  }

  override protected def getPartitions: Array[Partition] = {
    val num = frame.anyVec().nChunks()
    val res = new Array[Partition](num)
    for( i <- 0 until num ) res(i) = new Partition { val index = i }
    res
  }
}
