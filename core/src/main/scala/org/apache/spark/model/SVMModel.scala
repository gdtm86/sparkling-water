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

import hex.Model
import org.apache.spark.mllib.linalg.Vector
import water.api.KeyV3
import water.{Key, Keyed}

class SVMModel(val selfKey: Key[_ <: Keyed[_ <: Keyed[_ <: AnyRef]]],
               val params: SVMModel.SVMParameters,
               val output: SVMModel.SVMOutput)
  extends Model[SVMModel, SVMModel.SVMParameters, SVMModel.SVMOutput](selfKey, params, output) {

  override def makeMetricBuilder(domain: Array[String]) = ???

  // TODO use Spark SVM predict() method here? Will have to rewrite it for toJavaPredictBody() anyway
  override def score0(data: Array[Double], preds: Array[Double]): Array[Double] = ???
}

object SVMModel {

  class SVMParameters extends Model.Parameters {

    override def fullName(): String = "Support Vector Machine"

    override def progressUnits(): Long = _max_iterations

    override def algoName(): String = "SVM"

    override def javaName(): String = classOf[SVMModel].getName

    var _step_size: Double = _
    var _max_iterations: Int = _
    var _reg_param: Double = _
    var _mini_batch_fraction: Double = _

    var _optimizer: String = _
    var _add_intercept: Boolean = _

    var _user_points: KeyV3.FrameKeyV3 = _ // TODO this should be either frame or Spark DataFrame

  }

  class SVMOutput(val interceptor: Int,
                  val weights: Vector) extends Model.Output {

    var iterations: Int = _

    var _training_time_ms: Array[Long] = Array[Long](System.currentTimeMillis)

  }

}
