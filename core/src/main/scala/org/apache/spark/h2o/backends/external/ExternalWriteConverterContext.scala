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

package org.apache.spark.h2o.backends.external

import org.apache.spark.h2o._
import org.apache.spark.h2o.converters.WriteConverterContext
import org.apache.spark.h2o.utils.NodeDesc
import water.{AutoBuffer, AutoBufferUtils, ExternalFrameHandler, UDP}

class ExternalWriteConverterContext(val nodeDesc: NodeDesc) extends ExternalBackendUtils with WriteConverterContext{

  var socketChannel = getConnection(nodeDesc)
  var rowCounter: Long = 0

  private def writeToChannel(ab: AutoBuffer): Unit = {
    val bb = AutoBufferUtils.exactSizeByteBuffer(ab)
    while (bb.hasRemaining) {
      socketChannel.write(bb)
    }
  }

  override def numOfRows(): Long = rowCounter

  private def closeSocket(): Unit = {
    socketChannel.close()
  }

  override def closeChunks(): Unit = {
    val ab = new AutoBuffer()
    ab.putInt(ExternalFrameHandler.CLOSE_NEW_CHUNK)
    writeToChannel(ab)

    // close socket since this was last job we had to do on remote node
    closeSocket()
  }

  override def createChunks(keystr: String, vecTypes: Array[Byte], chunkId: Int): Unit = {
    val ab = new AutoBuffer()
    AutoBufferUtils.putUdp(UDP.udp.frame_create, ab)
    ab.putInt(ExternalFrameHandler.CREATE_NEW_CHUNK)
    ab.putStr(keystr)
    ab.putA1(vecTypes)
    ab.putInt(chunkId)
    writeToChannel(ab)
  }


  override def put(columnNum: Int, n: Number) = {
    val ab = new AutoBuffer()
    ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
    ab.putInt(ExternalFrameHandler.TYPE_NUM)
    ab.putInt(columnNum)
    ab.put8d(n.doubleValue())
    writeToChannel(ab)
  }


  override def put(columnNum: Int, n: Boolean) = {
    val ab = new AutoBuffer()
    ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
    ab.putInt(ExternalFrameHandler.TYPE_NUM)
    ab.putInt(columnNum)
    ab.put8d(if (n) 1 else 0)
    writeToChannel(ab)
  }

  override def put(columnNum: Int, n: java.sql.Timestamp) = {
    val ab = new AutoBuffer()
    ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
    ab.putInt(ExternalFrameHandler.TYPE_NUM)
    ab.putInt(columnNum)
    ab.put8d(n.getTime())
    writeToChannel(ab)
  }

  override def put(columnNum: Int, n: String) = {
    val ab = new AutoBuffer()
    ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
    ab.putInt(ExternalFrameHandler.TYPE_STR)
    ab.putInt(columnNum)
    ab.putStr(n)
    writeToChannel(ab)
  }

  override def putNA(columnNum: Int) = {
    val ab = new AutoBuffer()
    ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
    ab.putInt(ExternalFrameHandler.TYPE_NA)
    ab.putInt(columnNum)
    writeToChannel(ab)
  }

  override def increaseRowCounter(): Unit = rowCounter = rowCounter + 1
}

object ExternalWriteConverterContext extends ExternalBackendUtils{

  def scheduleUpload[T](rdd: RDD[T]): (RDD[T], Map[Int, NodeDesc]) = {
    val nodes = cloudMembers

    val preparedRDD = if (rdd.getNumPartitions < nodes.length) {
      rdd.repartition(nodes.length)
    } else if (rdd.getNumPartitions > nodes.length) {
      // coalesce is more effective in this case since we're decreasing num of partitions - no extra shuffle
      rdd.coalesce(nodes.length, shuffle = false)
    } else {
      rdd
    }

    val uploadPlan = nodes.zipWithIndex.map {
      p => p._2 -> p._1
    }.toMap

    (preparedRDD, uploadPlan)
  }
}
