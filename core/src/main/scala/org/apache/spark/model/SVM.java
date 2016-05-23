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
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import water.Job;
import water.Scope;
import water.fvec.H2OFrame;
import water.util.Log;

/**
 * TODO need to figure out all the constructors
 * For now this one because I use it in model API registration
 */
public class SVM extends ModelBuilder<SVMModel, SVMModel.SVMParameters, SVMModel.SVMOutput> {

    private SparkContext sc = H2OContext.getSparkContext();
    private H2OContext h2oContext = H2OContext.getOrCreate(sc);

    public SVM(boolean startup_once) {
        super(new SVMModel.SVMParameters(), startup_once);
    }

    public SVM(Job<?> job) {
        super(new SVMModel.SVMParameters(), job);
    }

    public SVM(SVMModel.SVMParameters parms) {
        super(parms);
        init(false);
    }

    @Override
    protected Driver trainModelImpl() {
        return new SVMDriver();
    }

    @Override
    public ModelCategory[] can_build() {
        return new ModelCategory[]{
                ModelCategory.Binomial
        };
    }


    @Override
    public void init(boolean expensive) {
        if (_parms._train == null && _parms.training_rdd != null) {
            // TODO implement
        }
        super.init(expensive);
        if (_parms._max_iterations < 1 || _parms._max_iterations > 9999999) {
            error("max_iterations", "must be between 1 and 10 million");
        }
        // TODO validate other params. Optmizer name etc?
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

                RDD<LabeledPoint> training = getTrainingData(_parms);
                training.cache();

                org.apache.spark.mllib.classification.SVMModel trainedModel =
                        SVMWithSGD.train(training, _parms._max_iterations);

                training.unpersist(false);

                // Fill in the model
                model._output.weights = trainedModel.weights().toArray();
                model._output.interceptor = trainedModel.intercept();
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

        private RDD<LabeledPoint> getTrainingData(SVMModel.SVMParameters parms) {
            return h2oContext.asLabPointRDD(new H2OFrame(parms.train()))
                    .toJavaRDD()
                    .map(x -> {
                        Vector v = Vectors.dense(
                                (Double) x.Vector0().get(),
                                (Double) x.Vector1().get(),
                                (Double) x.Vector2().get(),
                                (Double) x.Vector3().get(),
                                (Double) x.Vector4().get()
                        );
                        return new LabeledPoint((Integer) x.Label().get(), v);
                    }).rdd();
        }
    }

}

