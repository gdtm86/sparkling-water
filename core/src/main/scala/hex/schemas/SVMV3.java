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
import water.api.ModelParametersSchema;


// Seems like this has to be in Java since H2O's frameworks uses reflection's getFields...
public class SVMV3 extends ModelBuilderSchema<SVM, SVMV3, SVMV3.SVMParametersV3> {

    public static final class SVMParametersV3 extends
            ModelParametersSchema<SVMModel.SVMParameters, SVMParametersV3> {
        public static String[] fields = new String[]{
                "model_id",
                "training_frame",
                "response_column",
                "validation_frame",
                "nfolds",
                "add_intercept",

                "step_size",
                "reg_param",
                "convergence_tol",
                "mini_batch_fraction",
                "add_feature_scaling",
                "threshold",

                "ignored_columns",
                "ignore_const_cols"
        };

        @API(help="Add intercept.", direction=API.Direction.INPUT)
        public boolean add_intercept = false;

        @API(help="Set step size", direction=API.Direction.INPUT)
        public double step_size = 1.0;

        @API(help="Set regularization parameter", direction=API.Direction.INPUT)
        public double reg_param = 0.01;

        @API(help="Set convergence tolerance", direction=API.Direction.INPUT)
        public double convergence_tol = 0.001;

        @API(help="Set mini batch fraction", direction=API.Direction.INPUT)
        public double mini_batch_fraction = 1.0;

        @API(help="Add feature scaling", direction=API.Direction.INPUT)
        public boolean add_feature_scaling = false;

        // TODO what exactly does INOUT do?? Should this be only INPUT?
        @API(help="Set threshold that separates positive predictions from negative ones. NaN for raw prediction.", direction=API.Direction.INOUT)
        public double threshold = 0.0;

    }

}
