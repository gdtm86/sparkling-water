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

package water.api;

import hex.ModelBuilder;
import hex.schemas.SVMV3;
import org.apache.spark.SparkContext;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.model.SVM;
import water.Job;
import water.Key;
import water.util.HttpResponseStatus;
import water.util.PojoUtils;

public class SparkModelBuilderHandler extends Handler {

    private final SparkContext sc;
    private final H2OContext h2oContext;

    public SparkModelBuilderHandler(SparkContext sc, H2OContext h2oContext) {
        this.sc = sc;
        this.h2oContext = h2oContext;
    }

    /**
     * Will try a handler for one model for now and try to generalize it later
     */
    public SVMV3 handleSVM(int version, SVMV3 s) {
        String model_id = s.parameters.model_id.name;
        String algoName = s.algo;
        Key key = model_id == null ? ModelBuilder.defaultKey(algoName) : Key.make(model_id);

        Job job = new Job(key, ModelBuilder.javaName(s.algo), algoName);

        SVM mb = new SVM(job, sc, h2oContext);
        mb.init(false);
        _t_start = System.currentTimeMillis();

        mb.trainModel();

        _t_stop = System.currentTimeMillis();
        s.fillFromImpl(mb);
        PojoUtils.copyProperties(
                s.parameters,
                mb._parms,
                PojoUtils.FieldNaming.ORIGIN_HAS_UNDERSCORES,
                null,
                new String[]{"error_count", "messages"}
        );
        s.setHttpStatus(HttpResponseStatus.OK.getCode());
        return s;
    }
}