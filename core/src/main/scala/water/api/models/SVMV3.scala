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

package water.api.models

import hex.schemas.ModelBuilderSchema
import org.apache.spark.model.{SVM, SVMModel}
import water.api.{API, KeyV3, ModelParametersSchema}

class SVMV3 extends ModelBuilderSchema[SVM, SVMV3, SVMV3.SVMParametersV3] {

}

object SVMV3 {

  final class SVMParametersV3 extends ModelParametersSchema[SVMModel.SVMParameters, SVMParametersV3] {
    @API(help = "Maximum training iterations.") var max_iterations: Int = 0
  }

  object SVMParametersV3 {
    // TODO copied from KMeans, check if all necessary, add missing ones
    // will definitely need to add options for the optimizer
    val fields: Array[String] = Array(
      "model_id",
      "training_frame",
      "validation_frame",
      "nfolds",
      "keep_cross_validation_predictions",
      "keep_cross_validation_fold_assignment",
      "fold_assignment",
      "fold_column",
      "ignored_columns",
      "ignore_const_cols",
      "score_each_iteration",
      "user_points",
      "max_iterations",
      "max_runtime_secs"
    )

    // Input fields
    @API(help = "User-specified points", required = false)
    var user_points: KeyV3.FrameKeyV3 = null

    @API(help="Maximum training iterations", gridable = true)
    var max_iterations: Int = 0

  }

}