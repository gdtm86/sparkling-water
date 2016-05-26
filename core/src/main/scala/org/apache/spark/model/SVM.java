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
import hex.ModelMetrics;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.optimization.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import water.Job;
import water.Scope;
import water.fvec.Frame;
import water.fvec.H2OFrame;
import water.fvec.Vec;
import water.util.Log;

/**
 * TODO need to figure out all the constructors
 * Maybe if I don't need them all I can rewrite this in Scala?
 */
public class SVM extends ModelBuilder<SVMModel, SVMModel.SVMParameters, SVMModel.SVMOutput> {

    public enum Gradient {
        Hinge(new HingeGradient()),
        LeastSquares(new LeastSquaresGradient()),
        Logistic(new LogisticGradient());

        private org.apache.spark.mllib.optimization.Gradient sparkGradient;

        Gradient(org.apache.spark.mllib.optimization.Gradient sparkGradient) {
            this.sparkGradient = sparkGradient;
        }

        public org.apache.spark.mllib.optimization.Gradient get() {
            return sparkGradient;
        }
    }

    public enum Updater {
        L2(new SquaredL2Updater()),
        L1(new L1Updater()),
        Simple(new SimpleUpdater());

        private org.apache.spark.mllib.optimization.Updater sparkUpdater;

        Updater(org.apache.spark.mllib.optimization.Updater sparkUpdater) {
            this.sparkUpdater = sparkUpdater;
        }

        public org.apache.spark.mllib.optimization.Updater get() {
            return sparkUpdater;
        }

    }

    private final SparkContext sc = H2OContext.getSparkContext();
    private final H2OContext h2oContext = H2OContext.getOrCreate(sc);
    private final SQLContext sqlContext = SQLContext.getOrCreate(sc);

    public SVM(boolean startup_once) {
        super(new SVMModel.SVMParameters(), startup_once);
        _nclass = Double.isNaN(_parms._threshold) ? 1 : 2;
    }
    public SVM(SVMModel.SVMParameters parms, Job job) {
        super(parms, job);
        init(false);
        _nclass = Double.isNaN(_parms._threshold) ? 1 : 2;
    }
    public SVM(SVMModel.SVMParameters parms) {
        super(parms);
        init(false);
        _nclass = Double.isNaN(_parms._threshold) ? 1 : 2;
    }

    @Override
    protected Driver trainModelImpl() {
        return new SVMDriver();
    }

    @Override
    public ModelCategory[] can_build() {
        return new ModelCategory[]{
                ModelCategory.Binomial,
                ModelCategory.Regression
        };
    }

    @Override
    public boolean isSupervised() {
        return true;
    }

    @Override
    public void init(boolean expensive) {
        super.init(expensive);
        if (_parms._max_iterations < 0 || _parms._max_iterations > 1e6)
            error("_max_iterations", " max_iterations must be between 0 and 1e6");
        if (_train == null) return;
        if (null != _parms._initial_weights) {
            Frame user_points = _parms._initial_weights.get();
            // -1 because of response column
            if (user_points.numCols() != _train.numCols() - numSpecialCols()) {
                error("_user_y", "The user-specified points must have the same number of columns (" + (_train.numCols() - numSpecialCols()) + ") as the training observations");
            }
        }
    }

    @Override
    public int numSpecialCols() { return (hasOffsetCol() ? 1 : 0) +
            (hasWeightCol() ? 1 : 0) +
            (hasFoldCol() ? 1 : 0) +
            // response column
            1; }

    private class SVMDriver extends Driver {

        @Override
        public void compute2() {
            SVMModel model = null;
            try {
                Scope.enter();
                _parms.read_lock_frames(_job);
                init(true);

                // The model to be built
                model = new SVMModel(dest(), _parms, new SVMModel.SVMOutput(SVM.this));
                model.delete_and_lock(_job);

                RDD<LabeledPoint> training = getTrainingData(
                        _train,
                        _parms._response_column,
                        model._output.nfeatures()
                );
                training.cache();

                SVMWithSGD svm = new SVMWithSGD();
                svm.setIntercept(_parms._add_intercept);
                svm.setFeatureScaling(_parms._add_feature_scaling);

                svm.optimizer().setNumIterations(_parms._max_iterations);

                svm.optimizer().setStepSize(_parms._step_size);
                svm.optimizer().setRegParam(_parms._reg_param);
                svm.optimizer().setMiniBatchFraction(_parms._mini_batch_fraction);
                svm.optimizer().setConvergenceTol(_parms._convergence_tol);
                svm.optimizer().setGradient(_parms._gradient.get());
                svm.optimizer().setUpdater(_parms._updater.get());

                org.apache.spark.mllib.classification.SVMModel trainedModel;
                if (null == _parms._initial_weights) {
                    trainedModel = svm.run(training);
                } else {
                    // TODO check if anyVec is null in param validation
                    trainedModel = svm.run(training, vec2vec(_parms.initialWeights().vecs()));
                }
                training.unpersist(false);

                model._output.weights = trainedModel.weights().toArray();
                model._output.interceptor = trainedModel.intercept();
                model.update(_job); // Update model in K/V store
                _job.update(model._parms._max_iterations); // TODO how to update from Spark hmmm?

                if (_valid != null) {
                    model.score(_parms.valid()).delete();
                    model._output._validation_metrics = ModelMetrics.getFromDKV(model,_parms.valid());
                    model.update(_job);
                }

                Log.info(model._output._model_summary);
            } finally {
                if (model != null) model.unlock(_job);
                _parms.read_unlock_frames(_job);
                Scope.exit();
            }
            tryComplete();
        }

        // TODO a better way to do this?
        private Vector vec2vec(Vec[] vals) {
            int chunks = vals.length;
            double[] weights = new double[chunks];
            for (int i = 0; i < chunks; i++) {
                weights[i] = vals[i].at(0);
            }
            return Vectors.dense(weights);
        }

        private RDD<LabeledPoint> getTrainingData(Frame parms, String _response_column, int nfeatures) {
            DataFrame df = h2oContext.createH2OSchemaRDD(new H2OFrame(parms), SVM.this.sqlContext);
            return df.toJavaRDD().map(new RowToVector(_response_column, nfeatures)).rdd();
        }

    }

    private static class RowToVector implements Function<Row, LabeledPoint> {

        private final String respCol;
        private final int nfeatures;

        private RowToVector(String resp, int nfeatures) {
            this.respCol = resp;
            this.nfeatures = nfeatures;
        }

        @Override
        public LabeledPoint call(Row r) throws Exception {
            // assuming the response was moved by ModelBuilder#init()
            double[] features = new double[nfeatures];
            for(int i = 0; i < nfeatures; i++) {
                features[i] = r.getDouble(i);
            }
            return new LabeledPoint(r.<Byte>getAs(respCol), Vectors.dense(features));
        }
    }
}

