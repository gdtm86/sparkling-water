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

import hex.Model;
import hex.ModelMetrics;
import hex.ModelMetricsBinomial;
import org.apache.spark.mllib.optimization.*;
import water.Key;
import water.codegen.CodeGeneratorPipeline;
import water.fvec.Frame;
import water.util.JCodeGen;
import water.util.SBPrintStream;

public class SVMModel extends Model<SVMModel, SVMModel.SVMParameters, SVMModel.SVMOutput> {

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

    public static class SVMParameters extends Model.Parameters {
        public String algoName() {
            return "SVM";
        }

        public String fullName() {
            return "Support Vector Machine";
        }

        public String javaName() {
            return SVMModel.class.getName();
        }

        @Override
        public long progressUnits() {
            return _max_iterations;
        }

        public final Frame initialWeights() { return null == _initial_weights ? null : _initial_weights.get(); }

        public int _max_iterations = 1000; // Max iterations
        public boolean _add_intercept = false;
        public double _step_size = 1.0;
        public double _reg_param = 0.01;
        public double _convergence_tol = 0.001;
        public double _mini_batch_fraction = 1.0;
        public double _threshold = 0.0;
        public Updater _updater = Updater.L2;
        public Gradient _gradient = Gradient.Hinge;
        public Key<Frame> _initial_weights;
    }

    public static class SVMOutput extends Model.Output {
        public double interceptor;
        public double[] weights;

        public SVMOutput(SVM b) {
            super(b);
        }

    }

    SVMModel(Key selfKey, SVMModel.SVMParameters parms, SVMModel.SVMOutput output) {
        super(selfKey, parms, output);
    }

    @Override
    public ModelMetrics.MetricBuilder makeMetricBuilder(String[] domain) {
        return new ModelMetricsBinomial.MetricBuilderBinomial(domain);
    }

    @Override
    protected double[] score0(double data[/*ncols*/], double preds[/*nclasses+1*/]) {
        java.util.Arrays.fill(preds,0);
        preds[0] = _output.interceptor;
        final double threshold = _parms._threshold;
        for(int i = 0; i < data.length; i++) {
            preds[0] += (data[i] * _output.weights[i]);
        }
        // TODO should this return only the predicted class in preds[0] or should I somehow calculate pred[1] and preds[2]?
        preds[0] = Double.isNaN(threshold) ? preds[0] : (preds[0] > threshold ? 1 : 0);
        return preds;
    }

    @Override protected SBPrintStream toJavaInit(SBPrintStream sb, CodeGeneratorPipeline fileCtx) {
        sb = super.toJavaInit(sb, fileCtx);
        sb.ip("public boolean isSupervised() { return " + isSupervised() + "; }").nl();
        JCodeGen.toStaticVar(sb, "WEIGHTS", _output.weights, "Weights.");
        return sb;
    }
    @Override protected void toJavaPredictBody(SBPrintStream bodySb,
                                               CodeGeneratorPipeline classCtx,
                                               CodeGeneratorPipeline fileCtx,
                                               final boolean verboseCode) {
        /**/bodySb.i().p("java.util.Arrays.fill(preds,0);").nl();
        /**/bodySb.i().p("preds[0] = ").p(_output.interceptor).p(";").nl();
        /**/bodySb.i().p("final double threshold = ").p(_parms._threshold).p(";").nl();
        /**/bodySb.i().p("for(int i = 0; i < data.length; i++) {").nl();
        /*  */bodySb.i(1).p("preds[0] += (data[i] * WEIGHTS[i]);").nl();
        /**/bodySb.i().p("}").nl();
        /**/bodySb.i().p("preds[0] = Double.isNaN(threshold) ? preds[0] : (preds[0] > threshold ? 1 : 0);").nl();
    }
}
