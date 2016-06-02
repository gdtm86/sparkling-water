package org.apache.spark.model;

import org.apache.spark.mllib.optimization.HingeGradient;
import org.apache.spark.mllib.optimization.LeastSquaresGradient;
import org.apache.spark.mllib.optimization.LogisticGradient;

// TODO need to figure out how to Scalafy this so the FlowUI works.
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
