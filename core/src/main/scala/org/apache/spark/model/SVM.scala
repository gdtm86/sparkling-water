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

import hex.ModelBuilder.BuilderVisibility
import hex.{ModelBuilder, ModelCategory}
import org.apache.spark.model.SVMModel.{SVMOutput, SVMParameters}

/**
  * TODO need to figure out all the constructors
  * For now this one because I use it in model API registration
 */
class SVM(val startupOnce: Boolean) extends
  SparkModelBuilder[SVMModel, SVMModel.SVMParameters, SVMOutput](new SVMParameters(), startupOnce) {

  // TODO should this also return a Driver? probably no since we dont use the MR framework
  override protected def trainSparkModel(): Unit = ???

  // TODO this will most probabbly not be needed for nonH2O models needs refactoring in H2O core
  override def trainModelImpl(): Driver = ???

  override def can_build(): Array[ModelCategory] =
    // TODO check with smarter people :-)
    Array(
      ModelCategory.Regression,
      ModelCategory.Binomial,
      ModelCategory.Multinomial
    )

  override def init(expensive: Boolean):Unit = {
    super.init(expensive)
    if (_parms._max_iterations < 1 || _parms._max_iterations > 9999999) {
      error("max_iterations", "must be between 1 and 10 million")
    }
    // TODO validate other params. Optmizer name etc?
  }

  override def builderVisibility = BuilderVisibility.Experimental
}
