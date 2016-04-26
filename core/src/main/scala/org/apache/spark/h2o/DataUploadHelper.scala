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

package org.apache.spark.h2o

import java.net.InetSocketAddress
import java.nio.{ByteOrder, ByteBuffer}
import java.nio.channels.SocketChannel

import org.apache.spark.h2o.H2OContextUtils.NodeDesc
import water.{ExternalFrameHandler, UDP, AutoBufferUtils, AutoBuffer}

object DataUploadHelper {
  private final val BB_BIG_SIZE = 64 * 1024

  def scheduleUpload[T](rdd: RDD[T]): (Map[Int, NodeDesc], RDD[T]) = {
    val nodes = H2OContextUtils.cloudMembers

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

    (uploadPlan, preparedRDD)
  }

  def getConnection(nodeDesc: NodeDesc): SocketChannel = {
    val sock = SocketChannel.open()
    sock.socket().setReuseAddress(true)
    sock.socket().setSendBufferSize(BB_BIG_SIZE)
    val isa = new InetSocketAddress(nodeDesc.hostname, nodeDesc.port + 1) // +1 to connect to internal comm port
    val res = sock.connect(isa) // Can toss IOEx, esp if other node is still booting up
    assert(res)
    sock.configureBlocking(true)
    assert(!sock.isConnectionPending && sock.isBlocking && sock.isConnected && sock.isOpen)
    sock.socket().setTcpNoDelay(true)
    val bb = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder())
    bb.put(2.asInstanceOf[Byte]).putChar(sock.socket().getLocalPort.asInstanceOf[Char]).put(0xef.asInstanceOf[Byte]).flip()
    while (bb.hasRemaining) {
      // Write out magic startup sequence
      sock.write(bb)
    }
    sock
  }


  class ConnectionHolder(val nodeDesc: NodeDesc) {
    var socketChannel = getConnection(nodeDesc)
    var rowCounter: Long = 0

    def increaseRowCounter(): Unit = {
      rowCounter = rowCounter + 1
    }

    def writeToChannel(ab: AutoBuffer): Unit = {
      val bb = AutoBufferUtils.exactSizeByteBuffer(ab)
      while (bb.hasRemaining) {
        socketChannel.write(bb)
      }
    }
    def numOfRows: Long = {
      rowCounter
    }

    def close(): Unit = {
      socketChannel.close()
    }

    def closeChunksRemotely(): Unit = {
      val ab = new AutoBuffer()
      ab.putInt(ExternalFrameHandler.CLOSE_NEW_CHUNK)
      writeToChannel(ab)
    }

    def createChunksRemotely(keystr: String, vecTypes: Array[Byte], chunkId: Int): Unit = {
      val ab = new AutoBuffer()
      AutoBufferUtils.putUdp(UDP.udp.frame_create, ab)
      ab.putInt(ExternalFrameHandler.CREATE_NEW_CHUNK)
      ab.putStr(keystr)
      ab.putA1(vecTypes)
      ab.putInt(chunkId)
      writeToChannel(ab)
    }


    def put(columnNum: Int, n: Number) = {
      val ab = new AutoBuffer()
      ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
      ab.putInt(ExternalFrameHandler.TYPE_NUM)
      ab.putInt(columnNum)
      ab.put8d(n.doubleValue())
      writeToChannel(ab)
    }


    def put(columnNum: Int, n: Boolean) = {
      val ab = new AutoBuffer()
      ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
      ab.putInt(ExternalFrameHandler.TYPE_NUM)
      ab.putInt(columnNum)
      ab.put8d(if (n) 1 else 0)
      writeToChannel(ab)
    }

    def put(columnNum: Int, n: java.sql.Timestamp) = {
      val ab = new AutoBuffer()
      ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
      ab.putInt(ExternalFrameHandler.TYPE_NUM)
      ab.putInt(columnNum)
      ab.put8d(n.getTime())
      writeToChannel(ab)
    }

    def put(columnNum: Int, n: String) = {
      val ab = new AutoBuffer()
      ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
      ab.putInt(ExternalFrameHandler.TYPE_STR)
      ab.putInt(columnNum)
      ab.putStr(n)
      writeToChannel(ab)
    }

    def putNA(columnNum: Int) = {
      val ab = new AutoBuffer()
      ab.putInt(ExternalFrameHandler.ADD_TO_FRAME)
      ab.putInt(ExternalFrameHandler.TYPE_NA)
      ab.putInt(columnNum)
      writeToChannel(ab)
    }

  }

}
