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

import hex.Model.{Output, Parameters}
import hex.{Model, ModelBuilder}
import water.Key
import water.util.ArrayUtils

trait SparkModelBuilder extends ModelBuilder {

  // TODO should this also return a Driver? probably no since we dont use the MR framework
  def trainSparkModel()

}

object SparkModelBuilder {
  def make[B <: ModelBuilder](algo: String, result: Key[_ <: Model[_ <: Model[_, _, _], _ <: Parameters, _ <: Output]]): B = {
    val idx: Int = ArrayUtils.find(ModelBuilder.algos(), algo.toLowerCase)
    assert(idx != -1, "Unregistered algorithm " + algo)
    val mb: B = builders(idx).clone.asInstanceOf[B]
    // TODO set those somehow
//    mb._result = result
//    mb._parms = builders(idx)._parms.clone
    mb
  }

  // FIXME yes very bad I know but don't see better ways to do it without ModelBuilder refactoring
  private def builders = {
    val myClass = Class.forName(classOf[ModelBuilder].getName)
    val myField = myClass.getDeclaredField("BUILDERS")
    myField.get(null).asInstanceOf[Array[ModelBuilder]]
  }

}