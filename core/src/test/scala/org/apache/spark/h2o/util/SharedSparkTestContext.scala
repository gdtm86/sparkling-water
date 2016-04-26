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
package org.apache.spark.h2o.util

import java.net.InetAddress

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.Suite

/** This fixture create a Spark context once and share it over whole run of test suite. */
trait SharedSparkTestContext extends SparkTestContext with H2OForker { self: Suite =>

  def createSparkContext:SparkContext

  def launchH2OCluster(sc: SparkContext): H2OConf ={
    val cloudName = uniqueCloudName("test")
    val clientIP = InetAddress.getLocalHost.getHostAddress
    cloudProcesses = launchCloud(2, cloudName, clientIP)
    new H2OConf(sc.getConf).setCloudName(cloudName).setClientIP(clientIP)
  }

  def createH2OContext(sc: SparkContext, conf: H2OConf ):H2OContext = {
    new H2OContext(sc, conf)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = createSparkContext
    val conf = launchH2OCluster(sc)
    sqlc = SQLContext.getOrCreate(sc)
    hc = createH2OContext(sc, conf)
  }

  override protected def afterAll(): Unit = {
    stopCloud(cloudProcesses)
    cloudProcesses = null
    resetContext()
    super.afterAll()
  }
}
