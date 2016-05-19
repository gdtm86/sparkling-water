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

import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.backends.{SharedH2OConf, SharedBackendUtils}
import org.apache.spark.h2o.utils.NodeDesc
import water.H2O

/**
  * Various helper methods used in the external backend
  */
private[external] trait ExternalBackendUtils extends SharedBackendUtils{

  private final val BB_BIG_SIZE = 64 * 1024

  /**
    * Get arguments for H2O client
    *
    * @return array of H2O client arguments.
    */
  override def getH2OClientArgs(conf: H2OConf): Array[String] = {
    Array[String]("-md5skip")++getH2OClientConnectionArgs(conf)++super.getH2OClientArgs(conf)
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

  def cloudMembers = {
    H2O.CLOUD.members().map {
      node =>
        NodeDesc(node.index().toString, node.getIpPortString.split(":")(0), Integer.parseInt(node.getIpPortString.split(":")(1)))
    }
  }

  private[this] def getH2OClientConnectionArgs(conf: H2OConf): Array[String] = {
    if (conf.flatFilePath.isDefined) {
      Array("-flatfile", conf.flatFilePath.get)
    }else{
      Array()
    }
  }
}
private[external] object ExternalBackendUtils extends ExternalBackendUtils
