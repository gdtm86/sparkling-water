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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark._
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.h2o.H2OContextUtils._
import org.apache.spark.h2o.converters._
import org.apache.spark.rdd.{H2ORDD, H2OSchemaRDD}
import org.apache.spark.sql.{DataFrame, SQLContext}
import water._
import water.api.RestAPIManager

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.control.NoStackTrace

/**
 * Simple H2O context motivated by SQLContext.
 *
 * It provides implicit conversion from RDD -> H2OLikeRDD and back.
 */

/**
  * Create new H2OContext based on provided H2O configuration
  *
  * @param sparkContext Spark Context
  * @param conf H2O configuration
  */
class H2OContext (@transient val sparkContext: SparkContext, conf: H2OConf) extends org.apache.spark.Logging
  with Serializable with SparkDataFrameConverter with SupportedRDDConverter {
  self =>

  /**
    * Create new H2OContext based on provided H2O configuration.
    *
    * @param sc Spark Context
    * @param externalClusterMode when set to true it tries to connect to existing H2O cluster using the provided spark
    *                            configuration properties, otherwise it creates H2O cluster living in Spark - that means
    *                            that each Spark executor will have one h2o instance running in it.  This mode is not
    *                            recommended for big clusters and clusters where Spark executors are not stable.
    *
    *                            Setting this property as parameter has the same effect as setting the spark configuration
    *                            property spark.ext.h2o.mode which can be set in script starting sparkling-water or
    *                            can be set in H2O configuration class H2OConf
    */
  def this(sc: SparkContext, externalClusterMode: Boolean = true) = {
    this(sc, new H2OConf(sc.getConf).setMode(externalClusterMode = true))
  }

  /**
    * Create new H2OContext based on provided H2O configuration.
    *
    * This method automatically implies that H2OContext is created in external cluster mode
    * This method starts H2O context by connecting to existing H2O Cluster, which is specified by its name
    *
    * @param sc Spark Context
    * @param cloudName cloud name
    */
  def this(sc: SparkContext, cloudName: String) = this(sc, new H2OConf(sc.getConf).setCloudName(cloudName))

  /**
    * Create new H2OContext based on provided H2O configuration.
    *
    * This method automatically implies that H2OContext is created in external cluster mode
    * This method starts H2O context by connecting to existing H2O Cluster, which is specified by flat file
    * containing lines in a form:
    * node1_ip:node1_port
    * node2_ip:node2_port
    * ....
    *
    * @param sc Spark Context
    * @param flatFile flat file containing lines in format ip:port
    */
  def this(sc: SparkContext, cloudName: String, flatFile: String) = {
    this(sc, new H2OConf(sc.getConf).setCloudName(cloudName).setFlatFile(flatFile))
  }


  /**
    * This method connects to external H2O cluster if spark.ext.h2o.externalClusterMode is set to true,
    * otherwise it creates new H2O cluster living in Spark
    */
  def init(): Unit = {
    // Start H2O in client mode and connect to existing H2O Cluster
    // Check Spark and H2O environment, reconfigure values if necessary and warn about possible problems
    conf.checkAndUpdateSparkEnv()
    conf.checkAndUpdateH2OEnv()
    // Fix the state of H2OConf so it can't be changed anymore
    _conf = conf.clone()

    if(!isRunningOnCorrectSpark){
      throw new WrongSparkVersion(s"You are trying to use Sparkling Water built for Spark $buildSparkMajorVersion," +
        s" but your $$SPARK_HOME(=${sparkContext.getSparkHome().getOrElse("SPARK_HOME is not defined!")}) property" +
        s" points to Spark of version ${sparkContext.version}. Please ensure correct Spark is provided and" +
        s" re-run Sparkling Water.")
    }

    logTrace("Starting H2O on client mode and connecting it to existing h2o cluster")
    val h2oClientArgs = getH2OClientArgs(_conf)
    logDebug(s"Arguments used for launching h2o client node: ${h2oClientArgs.mkString(" ")}")
    H2OStarter.start(h2oClientArgs, false)

    // Register web API for client
    RestAPIManager.registerClientWebAPI(sparkContext, this)
    H2O.finalizeRegistration()

    // Fill information about H2O client and H2O nodes in the cluster
    h2oNodes.append(cloudMembers:_*)
    localClientIp = H2O.SELF_ADDRESS.getHostAddress
    localClientPort = H2O.API_PORT
    logInfo("Sparkling Water started, status of context: " + this.toString)

    // Store this instance so it can be obtained using getOrCreate method
    H2OContext.setInstantiatedContext(this)
  }


  /** H2O and Spark configuration */
  @transient private var _conf: H2OConf = _
  /** IP of H2O client */
  private var localClientIp: String = _
  /** REST port of H2O client */
  private var localClientPort: Int = _
  /** Runtime list of active H2O nodes */
  private val h2oNodes = mutable.ArrayBuffer.empty[NodeDesc]

  /**
    * Return a copy of this H2OContext's configuration. The configuration ''cannot'' be changed at runtime.
    */
  def getConf: H2OConf = _conf.clone()

  /** Transforms RDD[Supported type] to H2OFrame */
  def asH2OFrame(rdd: SupportedRDD): H2OFrame = asH2OFrame(rdd, None)
  def asH2OFrame(rdd: SupportedRDD, frameName: Option[String]): H2OFrame =  toH2OFrame(sparkContext, rdd, frameName)
  def asH2OFrame(rdd: SupportedRDD, frameName: String): H2OFrame = asH2OFrame(rdd, Option(frameName))


  /** Transforms RDD[Supported type] to H2OFrame key */
  def toH2OFrameKey(rdd: SupportedRDD): Key[_] = toH2OFrameKey(rdd, None)
  def toH2OFrameKey(rdd: SupportedRDD, frameName: Option[String]): Key[_] = asH2OFrame(rdd, frameName)._key
  def toH2OFrameKey(rdd: SupportedRDD, frameName: String): Key[_] = toH2OFrameKey(rdd, Option(frameName))

  /** Transform DataFrame to H2OFrame */
  def asH2OFrame(df : DataFrame): H2OFrame = asH2OFrame(df, None)
  def asH2OFrame(df : DataFrame, frameName: Option[String]) : H2OFrame = toH2OFrame(sparkContext, df, frameName)
  def asH2OFrame(df : DataFrame, frameName: String) : H2OFrame = asH2OFrame(df, Option(frameName))

  /** Transform DataFrame to H2OFrame key */
  def toH2OFrameKey(df : DataFrame): Key[Frame] = toH2OFrameKey(df, None)
  def toH2OFrameKey(df : DataFrame, frameName: Option[String]) : Key[Frame] = asH2OFrame(df, frameName)._key
  def toH2OFrameKey(df : DataFrame, frameName: String) : Key[Frame] = toH2OFrameKey(df, Option(frameName))

  /** Create a new H2OFrame based on existing Frame referenced by its key.*/
  def asH2OFrame(s: String): H2OFrame = new H2OFrame(s)

  /** Create a new H2OFrame based on existing Frame */
  def asH2OFrame(fr: Frame): H2OFrame = new H2OFrame(fr)

  /** Convert given H2O frame into a Product RDD type */
  def asRDD[A <: Product: TypeTag: ClassTag](fr : H2OFrame) : RDD[A] = createH2ORDD[A](fr)

  /** Convert given H2O frame into DataFrame type */
  def asDataFrame(fr : H2OFrame)(implicit sqlContext: SQLContext) : DataFrame = createH2OSchemaRDD(fr)
  def asDataFrame(fr: Frame)(implicit  sqlContext: SQLContext) : DataFrame = createH2OSchemaRDD(new H2OFrame(fr))
  def asDataFrame(s : String)(implicit sqlContext: SQLContext) : DataFrame = createH2OSchemaRDD(new H2OFrame(s))

  def h2oLocalClient = this.localClientIp + ":" + this.localClientPort

  def h2oLocalClientIp = this.localClientIp

  def h2oLocalClientPort = this.localClientPort

  // For now disable opening Spark UI
  //def sparkUI = sparkContext.ui.map(ui => ui.appUIAddress)

  /** Stops H2O context.
    *
    * Calls System.exit() which kills executor JVM.
    */
  def stop(stopSparkContext: Boolean = false): Unit = {
    if (stopSparkContext) sparkContext.stop()
    H2O.orderlyShutdown(1000)
    H2O.exit(0)
  }

  def createH2ORDD[A <: Product: TypeTag: ClassTag](fr: H2OFrame): RDD[A] = {
    new H2ORDD[A](this,fr)
  }

  def createH2OSchemaRDD(fr: H2OFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val h2oSchemaRDD = new H2OSchemaRDD(this, fr)
    import org.apache.spark.sql.H2OSQLContextUtils.internalCreateDataFrame
    internalCreateDataFrame(h2oSchemaRDD, H2OSchemaUtils.createSchema(fr))(sqlContext)
  }

  /** Open H2O Flow running in this client. */
  def openFlow(): Unit = openURI(s"http://$h2oLocalClient")
  /** Open Spark task manager. */
  //def openSparkUI(): Unit = sparkUI.foreach(openURI(_))

  /** Open browser for given address.
    *
    * @param uri addres to open in browser, e.g., http://example.com
    */
  private def openURI(uri: String): Unit = {
    import java.awt.Desktop
    if (!isTesting) {
      if (Desktop.isDesktopSupported) {
        Desktop.getDesktop.browse(new java.net.URI(uri))
      } else {
        logWarning(s"Desktop support is missing! Cannot open browser for $uri")
      }
    }
  }

  /**
   * Return true if running inside spark/sparkling water test.
   *
   * @return true if the actual run is test run
    * @return true if the actual run is test run
   */
  private def isTesting = sparkContext.conf.contains("spark.testing") || sys.props.contains("spark.testing")

  override def toString: String = {
    s"""
      |Sparkling Water Context:
      | * H2O name: ${H2O.ARGS.name}
      | * number of executors: ${h2oNodes.size}
      | * list of used executors:
      |  (executorId, host, port)
      |  ------------------------
      |  ${h2oNodes.mkString("\n  ")}
      |  ------------------------
      |
      |  Open H2O Flow in browser: http://$h2oLocalClient (CMD + click in Mac OSX)
    """.stripMargin
  }

  /** Checks whether version of provided Spark is the same as Spark's version designated for this Sparkling Water version.
    * We check for correct version in shell scripts and during the build but we need to do the check also in the code in cases when the user
    * executes for example spark-shell command with sparkling water assembly jar passed as --jars and initiates H2OContext.
    * (Because in that case no check for correct Spark version has been done so far.)
    */
  private def isRunningOnCorrectSpark = sparkContext.version.startsWith(buildSparkMajorVersion)

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /** Define implicits available via h2oContext.implicits._*/
  object implicits extends H2OContextImplicits with Serializable {
    protected override def _h2oContext: H2OContext = self
  }
  // scalastyle:on
}

object H2OContext extends Logging{

  private[H2OContext] def setInstantiatedContext(h2oContext: H2OContext): Unit = {
    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null) {
        instantiatedContext.set(h2oContext)
      }
    }
  }

  @transient private val instantiatedContext = new AtomicReference[H2OContext]()

  /**
    * Get existing H2O Context or None when it hasn't been started yet
    * @return Option containing H2O Context or None
    */
  def get(): Option[H2OContext] = Option(instantiatedContext.get())

  /**
    * Get existing or create new H2OContext based on provided H2O configuration
    *
    * @param sc Spark Context
    * @param conf H2O configuration
    * @return H2O Context
    */
  def getOrCreate(sc: SparkContext, conf: H2OConf): H2OContext = synchronized {
    if (instantiatedContext.get() == null) {
      instantiatedContext.set(new H2OContext(sc, conf))
      instantiatedContext.get().init()
    }
    instantiatedContext.get()
  }

  /**
    * Get existing or create new H2OContext based on provided H2O configuration.
    *
    * @param sc Spark Context
    * @param externalClusterMode when set to true it tries to connect to existing H2O cluster using the provided spark
    *                            configuration properties, otherwise it creates H2O cluster living in Spark - that means
    *                            that each Spark executor will have one h2o instance running in it.  This mode is not
    *                            recommended for big clusters and clusters where Spark executors are not stable.
    *
    *                            Setting this property as parameter has the same effect as setting the spark configuration
    *                            property spark.ext.h2o.mode which can be set in script starting sparkling-water or
    *                            can be set in H2O configuration class H2OConf
    * @return H2O Context
    */
  def getOrCreate(sc: SparkContext, externalClusterMode: Boolean = true): H2OContext = {
    getOrCreate(sc, new H2OConf(sc.getConf).setMode(externalClusterMode = true))
  }

  /**
    * Get existing or create new H2OContext based on provided H2O configuration.
    *
    * This method automatically implies that H2OContext is created in external cluster mode
    * This method starts H2O context by connecting to existing H2O Cluster, which is specified by its name
    *
    * @param sc Spark Context
    * @param cloudName cloud name
    * @return H2O Context
    */
  def getOrCreate(sc: SparkContext, cloudName: String): H2OContext = {
    getOrCreate(sc, new H2OConf(sc.getConf).setCloudName(cloudName))
  }

  /**
    * Get existing or create new H2OContext based on provided H2O configuration.
    *
    * This method automatically implies that H2OContext is created in external cluster mode
    * This method starts H2O context by connecting to existing H2O Cluster, which is specified by flat file
    * containing lines in a form:
    * node1_ip:node1_port
    * node2_ip:node2_port
    * ....
    *
    * @param sc Spark Context
    * @param flatFile flat file containing lines in format ip:port
    * @return H2O Context
    */
  def getOrCreate(sc: SparkContext, cloudName: String, flatFile: String): H2OContext = {
    getOrCreate(sc, new H2OConf(sc.getConf).setCloudName(cloudName).setFlatFile(flatFile))
  }
  
}

class WrongSparkVersion(msg: String) extends Exception(msg) with NoStackTrace