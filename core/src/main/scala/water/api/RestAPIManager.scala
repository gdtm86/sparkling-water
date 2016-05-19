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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import water.api.DataFrames.DataFramesHandler
import water.api.H2OFrames.H2OFramesHandler
import water.api.RDDs.RDDsHandler
import water.api.scalaInt.ScalaCodeHandler


object RestAPIManager {
  def registerClientWebAPI(h2oContext: H2OContext): Unit = {
    if(h2oContext.getConf.isH2OReplEnabled){
      registerScalaIntEndp(h2oContext.sparkContext, h2oContext)
    }
    registerDataFramesEndp(h2oContext.sparkContext, h2oContext)
    registerH2OFramesEndp(h2oContext.sparkContext, h2oContext)
    registerRDDsEndp(h2oContext.sparkContext, h2oContext)
  }

  private def registerH2OFramesEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val h2oFramesHandler = new H2OFramesHandler(sc, h2oContext)

    def h2oFramesFactory = new HandlerFactory {
      override def create(handler: Class[_ <: Handler]): Handler = h2oFramesHandler
    }

    RequestServer.register("/3/h2oframes/(?<h2oframe_id>.*)/dataframe", "POST",
      classOf[H2OFramesHandler], "toDataFrame",
      null,
      "Transform H2OFrame with given ID to Spark's DataFrame",
      h2oFramesFactory)

  }

  private def registerRDDsEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val rddsHandler = new RDDsHandler(sc, h2oContext)

    def rddsFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = rddsHandler
    }

    RequestServer.register("/3/RDDs", "GET",
      classOf[RDDsHandler], "list",
      null,
      "Return all RDDs within Spark cloud",
      rddsFactory)

    RequestServer.register("/3/RDDs/(?<rdd_id>[0-9]+)", "POST",
      classOf[RDDsHandler], "getRDD",
      null,
      "Get RDD with the given ID from Spark cloud",
      rddsFactory)

    RequestServer.register("/3/RDDs/(?<rdd_id>[0-9a-zA-Z_]+)/h2oframe", "POST",
      classOf[RDDsHandler], "toH2OFrame",
      null,
      "Transform RDD with the given ID to H2OFrame",
      rddsFactory)

  }

  private def registerDataFramesEndp(sc: SparkContext, h2oContext: H2OContext) = {

    val dataFramesHandler = new DataFramesHandler(sc, h2oContext)

    def dataFramesfactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = dataFramesHandler
    }

    RequestServer.register("/3/dataframes", "GET",
      classOf[DataFramesHandler], "list",
      null,
      "Return all Spark's DataFrames",
      dataFramesfactory)

    RequestServer.register("/3/dataframes/(?<dataframe_id>[0-9a-zA-Z_]+)", "POST",
      classOf[DataFramesHandler], "getDataFrame",
      null,
      "Get Spark's DataFrame with the given ID",
      dataFramesfactory)

    RequestServer.register("/3/dataframes/(?<dataframe_id>[0-9a-zA-Z_]+)/h2oframe", "POST",
      classOf[DataFramesHandler], "toH2OFrame",
      null,
      "Transform Spark's DataFrame with the given ID to H2OFrame",
      dataFramesfactory)

  }

  private def registerScalaIntEndp(sc: SparkContext, h2oContext: H2OContext) = {
    val scalaCodeHandler = new ScalaCodeHandler(sc, h2oContext)
    def scalaCodeFactory = new HandlerFactory {
      override def create(aClass: Class[_ <: Handler]): Handler = scalaCodeHandler
    }
    RequestServer.register("/3/scalaint/(?<session_id>[0-9]+)", "POST",
      classOf[ScalaCodeHandler], "interpret",
      null,
      "Interpret the code and return the result",
      scalaCodeFactory)

    RequestServer.register("/3/scalaint", "POST",
      classOf[ScalaCodeHandler], "initSession",
      null,
      "Return session id for communication with scala interpreter",
      scalaCodeFactory)

    RequestServer.register("/3/scalaint", "GET",
      classOf[ScalaCodeHandler], "getSessions",
      null,
      "Return all active session IDs",
      scalaCodeFactory)

    RequestServer.register("/3/scalaint/(?<session_id>[0-9]+)", "DELETE",
      classOf[ScalaCodeHandler], "destroySession",
      null,
      "Return session id for communication with scala interpreter",
      scalaCodeFactory)
  }
}
