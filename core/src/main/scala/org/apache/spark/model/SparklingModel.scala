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

package org.apache.spark.model

import java.io.FileOutputStream

import org.apache.spark.mllib.feature.{HashingTF, IDFModel}
import water.util.SBPrintStream

import scala.language.implicitConversions

object SparklingModel {

  implicit def sparkToSparkling(model: IDFModel): SparklingIDFModel = new SparklingIDFModel(model)

  implicit def sparkToSparkling(model: HashingTF): SparklingTFModel = new SparklingTFModel(model)

}

trait SparklingModel {

  val name: String

  def toJava(sb: SBPrintStream): SBPrintStream

  def save(): Unit = save(s"$name.java")

  def save(file: String): Unit = toJava(new SBPrintStream(new FileOutputStream(file)))
}
