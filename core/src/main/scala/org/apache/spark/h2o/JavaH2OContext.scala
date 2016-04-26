package org.apache.spark.h2o

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import water.Key

/**
  * A Java-friendly version of [[org.apache.spark.h2o.H2OContext]]
  *
  * Only one H2OContext may be active per JVM.
  */

/**
  * Create new JavaH2OContext based on existing H2O Context
  *
  * @param hc H2O Context
  */
class JavaH2OContext(val hc: H2OContext) {

  /**
    * Create new H2OContext based on provided H2O configuration
    *
    * @param sc Spark Context
    * @param conf H2O configuration
    */
  def this(sc: JavaSparkContext, conf: H2OConf) = this(new H2OContext(sc.sc, conf))

  /**
    * Create new H2OContext based on provided H2O configuration.
    *
    * @param sc Java Spark Context
    * @param externalClusterMode when set to true it tries to connect to existing H2O cluster using the provided spark
    *                            configuration properties, otherwise it creates H2O cluster living in Spark - that means
    *                            that each Spark executor will have one h2o instance running in it.  This mode is not
    *                            recommended for big clusters and clusters where Spark executors are not stable.
    *
    *                            Setting this property as parameter has the same effect as setting the spark configuration
    *                            property spark.ext.h2o.mode which can be set in script starting sparkling-water or
    *                            can be set in H2O configuration class H2OConf
    */
  def this(sc: JavaSparkContext, externalClusterMode: Boolean = true) = this(new H2OContext(sc.sc, externalClusterMode))

  /**
    * Create new H2OContext based on provided H2O configuration.
    *
    * This method automatically implies that H2OContext is created in external cluster mode
    * This method starts H2O context by connecting to existing H2O Cluster, which is specified by its name
    *
    * @param sc Java Spark Context
    * @param cloudName cloud name
    */
  def this(sc: JavaSparkContext, cloudName: String) = this(new H2OContext(sc.sc, new H2OConf(sc.getConf).setCloudName(cloudName)))

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
    * @param sc Java Spark Context
    * @param flatFile flat file containing lines in format ip:port
    */
  def this(sc: JavaSparkContext, cloudName: String, flatFile: String) = {
    this(new H2OContext(sc.sc, new H2OConf(sc.getConf).setCloudName(cloudName).setFlatFile(flatFile)))
  }


  /**
    * Return a copy of this JavaH2OContext's configuration. The configuration ''cannot'' be changed at runtime.
    */
  def getConf: H2OConf = hc.getConf

  /** Conversion from RDD[String] to H2O's DataFrame */
  def asH2OFrameFromRDDString(rdd: JavaRDD[String], frameName: String): H2OFrame = hc.toH2OFrame(hc.sparkContext, rdd.rdd, Option(frameName))

  /** Returns key of the H2O's DataFrame conversed from RDD[String]*/
  def asH2OFrameFromRDDStringKey(rdd: JavaRDD[String], frameName: String): Key[Frame] = asH2OFrameFromRDDString(rdd, frameName)._key

  /** Conversion from RDD[Boolean] to H2O's DataFrame */
  def asH2OFrameFromRDDBool(rdd: JavaRDD[Boolean], frameName: String): H2OFrame = hc.toH2OFrame(hc.sparkContext, rdd.rdd, Option(frameName))

  /** Returns key of the H2O's DataFrame conversed from RDD[Boolean]*/
  def asH2OFrameFromRDDBoolKey(rdd: JavaRDD[Boolean], frameName: String): Key[Frame] = asH2OFrameFromRDDBool(rdd, frameName)._key

  /** Conversion from RDD[Double] to H2O's DataFrame */
  def asH2OFrameFromRDDDouble(rdd: JavaRDD[Double], frameName: String): H2OFrame = hc.toH2OFrame(hc.sparkContext, rdd.rdd, Option(frameName))

  /** Returns key of the H2O's DataFrame conversed from RDD[Double]*/
  def asH2OFrameFromRDDDoubleKey(rdd: JavaRDD[Double], frameName: String): Key[Frame] = asH2OFrameFromRDDDouble(rdd, frameName)._key

  /** Conversion from RDD[Long] to H2O's DataFrame */
  def asH2OFrameFromRDDLong(rdd: JavaRDD[Long], frameName: String): H2OFrame = hc.toH2OFrame(hc.sparkContext, rdd.rdd, Option(frameName))

  /** Returns key of the H2O's DataFrame conversed from RDD[Long]*/
  def asH2OFrameFromRDDLongKey(rdd: JavaRDD[Long], frameName: String): Key[Frame] = asH2OFrameFromRDDLong(rdd, frameName)._key

}

object JavaH2OContext{

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
  def getOrCreate(sc: JavaSparkContext, externalClusterMode: Boolean = true): H2OContext = {
    val conf = new H2OConf(sc.getConf).setMode(externalClusterMode = true)
    getOrCreate(sc.sc, conf)
  }

  /**
    * Get existing or create new H2OContext based on provided H2O configuration
    *
    * @param sc Spark Context
    * @param conf H2O configuration
    * @return H2O Context
    */
  def getOrCreate(sc: JavaSparkContext, conf: H2OConf): H2OContext = synchronized {
    H2OContext.getOrCreate(sc.sc, conf)
  }

  /**
    * Get existing or create new H2OContext based on provided H2O configuration.
    * This method automatically implies that H2OContext is created in external cluster mode
    * This method starts H2O context by connecting to existing H2O Cluster, which is specified by its name
    *
    * @param sc Spark Context
    * @param cloudName cloud name
    * @return H2O Context
    */
  def getOrCreate(sc: JavaSparkContext, cloudName: String): H2OContext = {
    getOrCreate(sc.sc, new H2OConf(sc.getConf).setCloudName(cloudName))
  }

  /**
    * Get existing or create new H2OContext based on provided H2O configuration.
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
  def getOrCreate(sc: JavaSparkContext, cloudName: String, flatFile: String): H2OContext = {
    getOrCreate(sc.sc, new H2OConf(sc.getConf).setCloudName(cloudName).setFlatFile(flatFile))
  }


}