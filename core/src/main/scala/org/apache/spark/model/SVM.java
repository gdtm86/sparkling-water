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

package org.apache.spark.model;

import hex.ModelBuilder;
import hex.ModelCategory;
import org.apache.spark.SparkContext;
import org.apache.spark.h2o.H2OContext;
import water.Job;
import water.Scope;
import water.util.Log;

/**
 * TODO need to figure out all the constructors
 * For now this one because I use it in model API registration
 */
public class SVM extends ModelBuilder<SVMModel, SVMModel.SVMParameters, SVMModel.SVMOutput> {

    private SparkContext sc;
    private H2OContext h2OContext;

    public SVM(boolean startup_once) {
        super(new SVMModel.SVMParameters(),startup_once);
    }

    public SVM(Job<?> job,
               SparkContext sc,
               H2OContext h2OContext) {
        super(new SVMModel.SVMParameters(), job);
        this.sc = sc;
        this.h2OContext = h2OContext;
    }

    public SVM( SVMModel.SVMParameters parms ) { super(parms); init(false); }

    @Override
    protected Driver trainModelImpl() {
        return new SVMDriver();
    }

    @Override
    public ModelCategory[] can_build() {
        return new ModelCategory[]{
                ModelCategory.Regression,
                ModelCategory.Binomial,
                ModelCategory.Multinomial
        };
    }


    @Override
    public void init(boolean expensive) {
        super.init(expensive);
        if (_parms._max_iterations < 1 || _parms._max_iterations > 9999999) {
            error("max_iterations", "must be between 1 and 10 million");
        }
        // TODO validate other params. Optmizer name etc?
    }

    // TODO Experimental??
    @Override
    public BuilderVisibility builderVisibility() {
        return BuilderVisibility.Stable;
    }


    private class SVMDriver extends Driver {
        @Override
        public void compute2() {
            SVMModel model = null;
            try {
                Scope.enter();
                _parms.read_lock_frames(_job);
                init(true);

                // The model to be built
                model = new SVMModel(_job._result, _parms, new SVMModel.SVMOutput(SVM.this));
                model.delete_and_lock(_job);

//        _parms.train()

                // Fill in the model
//        model._output.weights =
//        model._output.interceptor =
                model.update(_job); // Update model in K/V store
//        _job.update(1); // TODO how to update from Spark hmmm?

                StringBuilder sb = new StringBuilder();
                sb.append("Example: iter: ").append(model._output._iterations);
                Log.info(sb);
            } finally {
                if (model != null) model.unlock(_job);
                _parms.read_unlock_frames(_job);
                Scope.exit(model == null ? null : model._key);
            }
            tryComplete();
        }
    }

}

