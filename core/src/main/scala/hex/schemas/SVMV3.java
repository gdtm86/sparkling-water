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

package hex.schemas;

import org.apache.spark.model.SVM;
import org.apache.spark.model.SVMModel;
import water.api.API;
import water.api.KeyV3;
import water.api.ModelParametersSchema;

public class SVMV3 extends ModelBuilderSchema<SVM, SVMV3, SVMV3.SVMParametersV3> {

    public static final class SVMParametersV3 extends
            ModelParametersSchema<SVMModel.SVMParameters, SVMParametersV3> {
        public static String[] fields = new String[]{
                "model_id",
                "training_frame",
                "training_rdd",
                "validation_frame",
                "nfolds",
                "keep_cross_validation_predictions",
                "keep_cross_validation_fold_assignment",
                "fold_assignment",
                "fold_column",
                "ignored_columns",
                "ignore_const_cols",
                "score_each_iteration",
                "max_runtime_secs",
                "max_iterations",
                "user_points"
        };

        @API(help="RDD used for training", required = false)
        public String training_rdd;

        @API(help = "User-specified points", required = false)
        public KeyV3.FrameKeyV3 user_points;

        @API(help = "Maximum training iterations", gridable = true)
        public int max_iterations;

    }

}
