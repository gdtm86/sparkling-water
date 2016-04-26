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

import org.apache.spark.h2o.H2OContextUtils._
import org.apache.spark.{SparkEnv, Logging, SparkConf}

/**
  * Just simple configuration holder which is representing
  * properties passed from user to Sparkling Water.
  */
class H2OConf(sparkConf: SparkConf) extends Logging{
  // Precondition
  require(sparkConf != null, "sparkConf was null")

  /* Initialize configuration */
  // Collect configuration properties
  import H2OConf._

  def setCloudName(cloudName: String): H2OConf = {
    sparkConf.set(PROP_CLOUD_NAME._1, cloudName)
    this
  }

  def setMode(externalClusterMode: Boolean = true): H2OConf ={
    sparkConf.set(PROP_MODE._1, externalClusterMode+"")
    this
  }
  def setClientIP(ip: String): H2OConf = {
    sparkConf.set(PROP_CLIENT_IP._1, ip)
    this
  }
  def setFlatFile(flatFile: String): H2OConf = {
    sparkConf.set(PROP_FLAT_FILE._1, flatFile)
    this
  }

  /** Copy this object */
  override def clone: H2OConf = {
    new H2OConf(sparkConf).setAll(getAll)
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    sparkConf.getAll
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): H2OConf = {
    sparkConf.setAll(settings)
    this
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = sparkConf.getInt(key, defaultValue)

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = sparkConf.getLong(key, defaultValue)

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = sparkConf.getDouble(key, defaultValue)

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = sparkConf.getBoolean(key, defaultValue)

  def clientIp      = sparkConf.getOption(PROP_CLIENT_IP._1)
  def flatFile = sparkConf.getOption(PROP_FLAT_FILE._1)
  def h2oClientLogDir = sparkConf.get(PROP_CLIENT_LOG_DIR._1, PROP_CLIENT_LOG_DIR._2)
  def networkMask   = sparkConf.getOption(PROP_NETWORK_MASK._1)
  def disableGA     = sparkConf.getBoolean(PROP_DISABLE_GA._1, PROP_DISABLE_GA._2)
  def clientWebPort = sparkConf.getInt(PROP_CLIENT_WEB_PORT._1, PROP_CLIENT_WEB_PORT._2)
  def clientIcedDir = sparkConf.getOption(PROP_CLIENT_ICED_DIR._1)
  def jks           = sparkConf.getOption(PROP_JKS._1)
  def jksPass       = sparkConf.getOption(PROP_JKS_PASS._1)
  def hashLogin     = sparkConf.getBoolean(PROP_HASH_LOGIN._1, PROP_HASH_LOGIN._2)
  def ldapLogin     = sparkConf.getBoolean(PROP_LDAP_LOGIN._1, PROP_LDAP_LOGIN._2)
  def loginConf     = sparkConf.getOption(PROP_LOGIN_CONF._1)
  def userName      = sparkConf.getOption(PROP_USER_NAME._1)
  def scalaIntDefaultNum = sparkConf.getInt(PROP_SCALA_INT_DEFAULT_NUM._1, PROP_SCALA_INT_DEFAULT_NUM._2)
  def isH2OReplEnabled = sparkConf.getBoolean(PROP_REPL_ENABLED._1,PROP_REPL_ENABLED._2)
  def isSparkVersionCheckEnabled = sparkConf.getBoolean(PROP_SPARK_VERSION_CHECK_ENABLED._1,PROP_SPARK_VERSION_CHECK_ENABLED._2)
  def clientBasePort = sparkConf.getInt(PROP_CLIENT_PORT_BASE._1, PROP_CLIENT_PORT_BASE._2)
  def cloudName     = sparkConf.getOption(PROP_CLOUD_NAME._1)
  def h2oClientLogLevel = sparkConf.get(PROP_CLIENT_LOG_LEVEL._1, PROP_CLIENT_LOG_LEVEL._2)
  def nthreads      = sparkConf.getInt(PROP_NTHREADS._1, PROP_NTHREADS._2)
  def mode = sparkConf.getBoolean(PROP_MODE._1, PROP_MODE._2)

  /** Set a configuration variable. */
  def set(key: String, value: String): H2OConf = {
    sparkConf.set(key, value)
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): H2OConf = {
    sparkConf.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = sparkConf.get(key)

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = sparkConf.get(key, defaultValue)

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = sparkConf.getOption(key)

  /** Check Spark environment and warn about possible problems. */
  private[h2o]
  def checkAndUpdateSparkEnv(): H2OConf = {
    // Increase locality timeout since h2o-specific tasks can be long computing
    if (getInt("spark.locality.wait", 3000) <= 3000) {
      logWarning(s"Increasing 'spark.locality.wait' to value 30000")
      set("spark.locality.wait", "30000")
    }
    this
  }

  /** Check H2O environment and warn about possible problems. */
  private[h2o]
  def checkAndUpdateH2OEnv(): H2OConf = {
    if(clientIp.isEmpty){
      set(PROP_CLIENT_IP._1,getHostname(SparkEnv.get))
    }
    this
  }


  override def toString: String =
    s"""Sparkling Water configuration:
         |  flatFile            : $flatFile
         |  cloudName           : $cloudName
         |  clientBasePort      : $clientBasePort
         |  h2oClientLog        : $h2oClientLogLevel
         |  nthreads            : $nthreads""".stripMargin

}
object H2OConf extends Logging{

  /**
    * When set to true H2O Context is created by connecting to existing H2O cluster, otherwise it creates
    * H2O cluster living in Spark - that means that each Spark executor will have one h2o instance running in it.
    * This mode is not recommended for big clusters and clusters where Spark executors are not stable.
    */
  val PROP_MODE = ("spark.ext.h2o.mode", true)
  /** Name of H2O cloud to connect to */
  val PROP_CLOUD_NAME = ("spark.ext.h2o.cloud.name", null.asInstanceOf[String])
  /** Path to flat file representing this cluster to which we should connect */
  val PROP_FLAT_FILE = ("spark.ext.h2o.cloud.flatfile",null.asInstanceOf[String])
  /* H2O internal log level for launched remote nodes. */
  val PROP_CLIENT_LOG_LEVEL = ("spark.ext.h2o.client.log.level", "DEBUG")
  /** Location of log directory for the driver instance. */
  val PROP_CLIENT_LOG_DIR = ("spark.ext.h2o.client.log.dir", defaultLogDir)
  /** Subnet selector for h2o if IP guess fail - useful if 'spark.ext.h2o.flatfile' is false
    * and we are trying to guess right IP on mi*/
  val PROP_NETWORK_MASK = ("spark.ext.h2o.network.mask", null.asInstanceOf[String])
  /** Limit for number of threads used by H2O, default -1 means unlimited */
  val PROP_NTHREADS = ("spark.ext.h2o.nthreads", -1)
  /** Disable GA tracking */
  val PROP_DISABLE_GA = ("spark.ext.h2o.disable.ga", true)
  /** Exact client port to access web UI.
    * The value `-1` means automatic search for free port starting at `spark.ext.h2o.port.base`. */
  val PROP_CLIENT_WEB_PORT = ("spark.ext.h2o.client.web.port", -1)
  /** Location of iced directory for the driver instance. */
  val PROP_CLIENT_ICED_DIR = ("spark.ext.h2o.client.iced.dir", null.asInstanceOf[String])
  /** FIXME: documentation UPDATE */
  val PROP_CLIENT_PORT_BASE = ( "spark.ext.h2o.client.port.base", 54321 )
  val PROP_CLIENT_IP = ("spark.ext.h2o.client.ip", null.asInstanceOf[String])
  /** Path to Java KeyStore file. */
  val PROP_JKS = ("spark.ext.h2o.jks", null.asInstanceOf[String])
  /** Password for Java KeyStore file. */
  val PROP_JKS_PASS = ("spark.ext.h2o.jks.pass", null.asInstanceOf[String])
  /** Enable hash login. */
  val PROP_HASH_LOGIN = ("spark.ext.h2o.hash.login", false)
  /** Enable LDAP login. */
  val PROP_LDAP_LOGIN = ("spark.ext.h2o.ldap.login", false)
  /** Login configuration file. */
  val PROP_LOGIN_CONF = ("spark.ext.h2o.login.conf", null.asInstanceOf[String])
  /** Override user name for cluster. */
  val PROP_USER_NAME = ("spark.ext.h2o.user.name", null.asInstanceOf[String])
  /** Number of executors started at the start of h2o services, by default 1 */
  val PROP_SCALA_INT_DEFAULT_NUM = ("spark.ext.scala.int.default.num",1)
  /** Enable/Disable Sparkling-Water REPL **/
  val PROP_REPL_ENABLED = ("spark.ext.h2o.repl.enabled",true)
  /** Enable/Disable check for Spark version. */
  val PROP_SPARK_VERSION_CHECK_ENABLED = ("spark.ext.h2o.spark.version.check.enabled",true)

  private[spark] def defaultLogDir: String = {
    System.getProperty("user.dir") + java.io.File.separator + "h2ologs"
  }
}
