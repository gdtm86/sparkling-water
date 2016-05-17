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

import hex.{ModelBuilder, ModelCategory}
import org.apache.spark.model.SVMModel.{SVMOutput, SVMParameters}

/**
  * TODO need to figure out all the constructors
  * For now this one because I use it in model API registration
 */
class SVM(val startupOnce: Boolean) extends
  ModelBuilder[SVMModel, SVMModel.SVMParameters, SVMOutput](new SVMParameters(), startupOnce) {

  // TODO guess this will need spark context? Need to check the driver class and how to integrate non MR jobs here
  override def trainModelImpl(): Driver = ???

  override def can_build(): Array[ModelCategory] = ???
}
