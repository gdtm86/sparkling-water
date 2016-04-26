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

import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.sys.process.Process
import scala.util.Random

/**
  * Used to start H2O nodes from scala code
  */
trait H2OForker extends BeforeAndAfterEach { self: Suite =>
  lazy val h2oJar = sys.props.getOrElse("H2O_JAR",
      fail("H2O_JAR environment variable is not set! It should point to the location of assembly jar file"))

    def uniqueCloudName(customPart: String) =  s"sparkling-water-$customPart-${Random.nextInt()}"

    private def launchSingle(cloudName: String, ip: String, basePort: Int): Process = {
      val cmdToLaunch = Seq[String]("java", "-jar", h2oJar, "-md5skip", "-name", cloudName, "-baseport", basePort.toString, "-ip", ip)
      import scala.sys.process._
      Process(cmdToLaunch).run()
    }

    def launchCloud(cloudSize: Int, cloudName: String, ip: String = InetAddress.getLocalHost.getHostAddress, basePort: Int = 54321): Seq[Process] ={
     (1 to cloudSize).map{ _ => launchSingle(cloudName, ip, basePort)}

    }

    def stopCloud(nodeProcesses: Seq[Process]): Unit ={
      nodeProcesses.foreach(_.destroy())
    }

}
