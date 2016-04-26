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

import org.apache.spark.h2o.util.SparkTestContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
 * Test passing parameters via SparkConf.
 */
@RunWith(classOf[JUnitRunner])
class H2OConfTestSuite extends FunSuite
with Matchers with BeforeAndAfter with SparkTestContext {

  //TODO: Make this test working
  test("test H2OConf parameters") {
    val sparkConf = new SparkConf()
      .set("spark.ext.h2o.flatfile", "false")
      .set("spark.ext.h2o.client.ip", "10.0.0.100")
      .set("spark.ext.h2o.client.port.base", "1267")
      .set("spark.ext.h2o.node.port.base", "32333")
      .set("spark.ext.h2o.cloud.name", "test-sparkling-cloud-")
      .set("spark.ext.h2o.client.log.level", "DEBUG")
      .set("spark.ext.h2o.client.log.level", "DEBUG")
      .set("spark.ext.h2o.network.mask", "127.0.0.1/32")
      .set("spark.ext.h2o.nthreads", "7")
      .set("spark.ext.h2o.disable.ga", "true")
      .set("spark.ext.h2o.client.web.port", "13321")


    sc = new SparkContext("local", "test-local", sparkConf)
    val conf = new H2OConf(sc.getConf).set("spark.ext.h2o.client.ip", "10.0.0.4").setCloudName("test")
    hc = new H2OContext(sc, conf)

    // Test passed values
    assert(hc.getConf.clientBasePort == 1267)
    assert(hc.getConf.clientIp == Some("10.0.0.100"))
    assert(hc.getConf.cloudName == "test-sparkling-cloud-")
    assert(hc.getConf.h2oClientLogLevel == "DEBUG")
    assert(hc.getConf.networkMask == Some("127.0.0.1/32"))
    assert(hc.getConf.nthreads == 7)
    assert(hc.getConf.disableGA == true)
    assert(hc.getConf.clientWebPort == 13321)

    resetContext()
  }
}
