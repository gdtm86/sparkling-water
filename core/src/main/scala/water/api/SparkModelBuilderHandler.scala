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

package water.api

import java.util.Properties

import hex.Model.{Output, Parameters}
import hex.schemas.{ModelBuilderSchema, SVMV3}
import hex.{Model, ModelBuilder}
import org.apache.spark.model.{SVM, SVMModel, SparkModelBuilder}
import water.util.{ArrayUtils, HttpResponseStatus, PojoUtils}
import water.{Key, TypeMap}

class SparkModelBuilderHandler[B <: ModelBuilder[_,_,_], S <: ModelBuilderSchema[_, _, _], P <: ModelParametersSchema[_,_]] extends Handler {

  /**
    * Will try a handler for one model for now and try to generalize it later
    */
  def handleSVM(version: Int, s: SVMV3): SVMV3 = {
    val model_id = s.parameters.model_id.name
    val algoName = s.algo
    val key: Key[SVMModel] =
      if (model_id == null) ModelBuilder.defaultKey(algoName).asInstanceOf[Key[SVMModel]]
      else Key.make(model_id)

    val idx: Int = ArrayUtils.find(ModelBuilder.algos(), algoName.toLowerCase)
    assert(idx != -1, "Unregistered algorithm " + algoName)
    val mb = new SVM(true)
    mb.initialize(algoName, key)
    mb.init(false)
    _t_start = System.currentTimeMillis
    mb.trainSparkModel()
    _t_stop = System.currentTimeMillis
    s.fillFromImpl(mb)
    PojoUtils.copyProperties(
      s.parameters,
      mb._parms,
      PojoUtils.FieldNaming.ORIGIN_HAS_UNDERSCORES,
      null,
      Array[String]("error_count", "messages")
    )
    s.setHttpStatus(HttpResponseStatus.OK.getCode)
    s
  }

  /**
    * TODO this will need some refactoring in H2O-3 I think, since the only change here is the call to:
    * builder.trainModel -> builder.trainModelImpl and the job
    * cannot override it since it's final... also making ModelBuilder more generic and subclassing it into
    * H2OModelBuilder and SparkModelBuilder might be a good idea since SparkModelBuilders won't need our H2O jobs
    *
    */
  // TODO figure out the types
  // Maybe I don't need to override this and just rely on the default handle from Handler??
  /*override*/ def handle2(version: Int, route: Route, parms: Properties): S = {
    val ss: Array[String] = route._url_pattern_raw.split("/")
    val algoURLName: String = ss(3)
    val algoName: String = ModelBuilder.algoName(algoURLName)
    val schemaDir: String = ModelBuilder.schemaDirectory(algoURLName)
    val schemaName: String = schemaDir + algoName + "V" + version
    val schema: S = TypeMap.newFreezable(schemaName).asInstanceOf[S]
    schema.init_meta()

    val parmName: String = schemaDir + algoName + "V" + version + "$" + algoName + "ParametersV" + version
    val parmSchema: P = TypeMap.newFreezable(parmName).asInstanceOf[P]
//    schema.parameters = parmSchema

    val handlerName: String = route._handler_method.getName
    val doTrain: Boolean = handlerName == "train"
    assert(doTrain || handlerName == "validate_parameters")

    val model_id: String = parms.getProperty("model_id")

    val key: Key[_ <: Model[_ <: Model[_, _, _], _ <: Parameters, _ <: Output]] =
      if (doTrain) {
        if (model_id == null) ModelBuilder.defaultKey(algoName)
        else Key.make(model_id)
      }
      else null

    val builder: B = SparkModelBuilder.make(algoURLName, key)

    // TODO fix those somehow
//    schema.parameters.fillFromImpl(builder._parms)
//    schema.parameters.fillFromParms(parms)
//    schema.parameters.fillImpl(builder._parms)

    builder.init(false)
    _t_start = System.currentTimeMillis

//    if (doTrain) builder.trainSparkModel()

    _t_stop = System.currentTimeMillis
//    schema.fillFromImpl(builder)
    PojoUtils.copyProperties(schema.parameters, builder._parms, PojoUtils.FieldNaming.ORIGIN_HAS_UNDERSCORES, null, Array[String]("error_count", "messages"))
    schema.setHttpStatus(HttpResponseStatus.OK.getCode)
    schema
  }

}

