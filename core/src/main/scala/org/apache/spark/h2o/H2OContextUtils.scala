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

import java.io.File


import org.apache.spark.SparkEnv
import water.H2O

/**
  * Support methods for H2OContext.
  */
private[spark] object H2OContextUtils {

  /** Helper class containing H2ONodeId, hostname and port */
  case class NodeDesc(H2ONodeId: Int, hostname: String, port: Int) {
    override def productPrefix = ""
  }

  /**
    * Return hostname of this node based on SparkEnv
    *
    * @param env SparkEnv instance
    * @return hostname of the node
    */
  def getHostname(env: SparkEnv) = env.blockManager.blockManagerId.host

  def cloudMembers = {
    H2O.CLOUD.members().map {
      node =>
        NodeDesc(node.index(), node.getIpPortString.split(":")(0), Integer.parseInt(node.getIpPortString.split(":")(1)))
    }
  }
  private[this] def getH2OClientConnectionArgs(conf: H2OConf): Array[String] = {
    if (conf.cloudName.isDefined) {
      if (conf.flatFile.isDefined) {
        Array("-name", conf.cloudName.get, "-flatfile", conf.flatFile.get)
      } else if (conf.cloudRepresentative.isDefined) {
        try {
          val ipPort = conf.cloudRepresentative.get
          val ip = ipPort.substring(ipPort.indexOf(":"))
          val port = Integer.parseInt(ipPort.substring(ipPort.indexOf(":") + 1, ipPort.length))
          Array("-name", conf.cloudName.get, "-flatfile", saveAsFile(ip + ":" + port).getAbsolutePath)
        } catch {
          case _: Throwable => throw new IllegalArgumentException("Wrong format of spark.ext.h2o.cloud.representative. It has to be formatted as ip:port")
        }
      } else {
        Array("-name", conf.cloudName.get)
      }
    } else {
      throw new IllegalArgumentException(
        """Cloud name has to be specified in all cases in order to connect to existing H2O Cloud via
          | spark.ext.h2o.cloud.name property or via one of the H2OContext's constructors""".stripMargin)
    }
  }

  /**
    * Get arguments for H2O client.
    *
    * @return array of H2O client arguments.
    */
  def getH2OClientArgs(conf: H2OConf): Array[String] = (
    getH2OClientConnectionArgs(conf) ++
    Seq(("-ga_opt_out", conf.disableGA)).filter(_._2).map(x => x._1) // Append single boolean options
      ++ Seq("-quiet")
      ++ Seq("-md5skip")
      ++ (if (conf.hashLogin) Seq("-hash_login") else Nil)
      ++ (if (conf.ldapLogin) Seq("-ldap_login") else Nil)
      ++ Seq("-log_level", conf.h2oClientLogLevel)
      ++ Seq("-log_dir", conf.h2oClientLogDir)
      ++ Seq("-baseport", conf.clientBasePort.toString)
      ++ Seq("-client")
      ++ Seq(
      ("-ip", conf.clientIp.get),
      ("-nthreads", if (conf.nthreads > 0) conf.nthreads else null),
      ("-network", conf.networkMask.orNull),
      ("-ice_root", conf.clientIcedDir.orNull),
      ("-port", if (conf.clientWebPort > 0) conf.clientWebPort else null),
      ("-jks", conf.jks.orNull),
      ("-jks_pass", conf.jksPass.orNull),
      ("-login_conf", conf.loginConf.orNull),
      ("-user_name", conf.userName.orNull)
    ).filter(_._2 != null).flatMap(x => Seq(x._1, x._2.toString))
    ).toArray

  def saveAsFile(content: String): File = {
    val tmpDir = createTempDir()
    tmpDir.deleteOnExit()
    val flatFile = new File(tmpDir, "flatfile.txt")
    val p = new java.io.PrintWriter(flatFile)
    try {
      p.print(content)
    } finally {
      p.close()
    }
    flatFile
  }

  val TEMP_DIR_ATTEMPTS = 1000

  private def createTempDir(): File = {
    def baseDir = new File(System.getProperty("java.io.tmpdir"))
    def baseName = System.currentTimeMillis() + "-"

    var cnt = 0
    while (cnt < TEMP_DIR_ATTEMPTS) {
      // infinite loop
      val tempDir = new File(baseDir, baseName + cnt)
      if (tempDir.mkdir()) return tempDir
      cnt += 1
    }
    throw new IllegalStateException(s"Failed to create temporary directory $baseDir / $baseName")
  }


  /**
    * Returns Major Spark version for which is this version of Sparkling Water designated.
    *
    * For example, for 1.6.1 returns 1.6
    */
  def buildSparkMajorVersion = {
    val stream = getClass.getResourceAsStream("/spark.version")
    val version = scala.io.Source.fromInputStream(stream).mkString
    if (version.count(_ == '.') == 1) {
      // e.g., 1.6
      version
    } else {
      // 1.4
      version.substring(0, version.lastIndexOf('.'))
    }
  }

}
