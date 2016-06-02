package org.apache.spark.model;

import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.optimization.SimpleUpdater;
import org.apache.spark.mllib.optimization.SquaredL2Updater;

// TODO need to figure out how to Scalafy this so the FlowUI works.
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
