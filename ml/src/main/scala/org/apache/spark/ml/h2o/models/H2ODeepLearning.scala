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

package org.apache.spark.ml.h2o.models

import java.io._

import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.ml.h2o.models.H2ODeepLearning.H2ODeepLearningWriter
import org.apache.spark.ml.h2o.models.H2ODeepLearningModel.H2ODeepLearningModelWriter
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.h2o.H2OKeyParam
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import water.Key
import water.app.ModelSerializationSupport
import water.fvec.Frame

/**
 * Deep learning ML component.
 */
class H2ODeepLearningModel(val model: DeepLearningModel, override val uid: String)(implicit h2OContext: H2OContext, sqlContext: SQLContext) extends Model[H2ODeepLearningModel] with MLWritable{

  def this(model: DeepLearningModel)(implicit h2OContext: H2OContext, sqlContext: SQLContext) = this(model,Identifiable.randomUID("dlModel"))

  override def copy(extra: ParamMap): H2ODeepLearningModel = defaultCopy(extra)
  override def transform(dataset: DataFrame): DataFrame = {
    import h2OContext.implicits._
    val frame: H2OFrame = dataset
    val prediction = model.score(frame)
    h2OContext.asDataFrame(prediction)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val ncols: Int = if (model._output.nclasses() == 1) 1 else model._output.nclasses() + 1
    StructType(model._output._names.map{
      name => StructField(name,DoubleType,nullable = true, metadata = null)
    })
  }

  @Since("1.6.0")
  override def write: MLWriter =  new H2ODeepLearningModelWriter(this)
}

object H2ODeepLearningModel extends MLReadable[H2ODeepLearningModel]{

  private final val modelFileName = "dl_model"

  private[H2ODeepLearningModel] class H2ODeepLearningModelWriter(instance: H2ODeepLearningModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val file = new File(path, modelFileName)
      ModelSerializationSupport.exportH2OModel(instance.model,file.toURI)
    }
  }

  private class H2ODeepLearningModelReader extends MLReader[H2ODeepLearningModel] {

    private val className = classOf[H2ODeepLearningModel].getName

    override def load(path: String): H2ODeepLearningModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val file = new File(path, modelFileName)
      val deepLearningModel = ModelSerializationSupport.loadH2OModel[DeepLearningModel](file.toURI)
      implicit val h2OContext = H2OContext.getOrCreate(sc)
      implicit val sQLContext = SQLContext.getOrCreate(sc)
      val h2oDeepLearning = new H2ODeepLearningModel(deepLearningModel, metadata.uid)
      DefaultParamsReader.getAndSetParams(h2oDeepLearning, metadata)
      h2oDeepLearning
    }
  }

  @Since("1.6.0")
  override def read: MLReader[H2ODeepLearningModel] = new H2ODeepLearningModelReader

  @Since("1.6.0")
  override def load(path: String): H2ODeepLearningModel = super.load(path)
}


/**
  *  Creates H2ODeepLearning model
  *  If the key specified the training set is specified using setTrainKey, then frame with this key is used as the
  *  training frame, otherwise it uses the frame from the previous stage as the training frame
  */
class H2ODeepLearning(deepLearningParameters: Option[DeepLearningParameters], override val uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends Estimator[H2ODeepLearningModel] with DeepLearningParams with MLWritable{

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("dl"))
  def this(deepLearningParameters: DeepLearningParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(deepLearningParameters),Identifiable.randomUID("dl"))
  def this(deepLearningParameters: DeepLearningParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(deepLearningParameters),uid)

  if(deepLearningParameters.isDefined){
    setDeepLearningParams(deepLearningParameters.get)
  }

  def allStringVecToCategorical(hf: H2OFrame): H2OFrame = {
    hf.vecs().indices
      .filter(idx => hf.vec(idx).isString)
      .foreach(idx => hf.replace(idx, hf.vec(idx).toCategoricalVec).remove())
    // Update frame in DKV
    water.DKV.put(hf)
    // Return it
    hf
  }

  override def fit(dataset: DataFrame): H2ODeepLearningModel = {
    import h2oContext.implicits._
    // check if trainKey is explicitly set
    val key = if(isSet(trainKey)){
      $(trainKey)
    }else{
      h2oContext.toH2OFrameKey(dataset)
    }
    setTrainKey(key)
    allStringVecToCategorical(key.get())
    val model = new DeepLearning(getDeepLearningParams).trainModel().get()
    val dlm = new H2ODeepLearningModel(model)
    dlm
  }

  /**
    * Set the param and execute custom piece of code
    */
  private def set[T](param: Param[T], value: T)(f:  => Unit): this.type ={
    f
    set(param,value)
  }

  /** @group setParam */
  def setEpochs(value: Double) = set(epochs, value){getDeepLearningParams._epochs = value}

  /** @group setParam */
  def setL1(value: Double) = set(l1, value){getDeepLearningParams._l1 = value}

  /** @group setParam */
  def setL2(value: Double) = set(l2, value){getDeepLearningParams._l2 = value}

  /** @group setParam */
  def setHidden(value: Array[Int]) = set(hidden, value){getDeepLearningParams._hidden = value}

  /** @group setParam */
  def setResponseColumn(value: String) = set(responseColumn,value){getDeepLearningParams._response_column = value}
  /** @group setParam */
  def setValidKey(value: String) = set(validKey,Key.make[Frame](value)){getDeepLearningParams._valid = Key.make[Frame](value)}
  /** @group setParam */
  def setValidKey(value: Key[Frame]) = set(validKey,value){getDeepLearningParams._valid = value}

  /** @group setParam */
  def setTrainKey(value: String) = set(trainKey,Key.make[Frame](value)){getDeepLearningParams._train = Key.make[Frame](value)}
  /** @group setParam */
  def setTrainKey(value: Key[Frame]) = set(trainKey,value){getDeepLearningParams._train = value}

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): Estimator[H2ODeepLearningModel] = defaultCopy(extra)

  @Since("1.6.0")
  override def write: MLWriter = new H2ODeepLearningWriter(this)
}

object H2ODeepLearning extends MLReadable[H2ODeepLearning]{

  private final val paramsFileName = "dl_params"

  private[H2ODeepLearning] class H2ODeepLearningWriter(instance: H2ODeepLearning) extends MLWriter{

    @Since("1.6.0") override protected
    def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val file = new File(path,paramsFileName)
      val oos = new ObjectOutputStream(new FileOutputStream(file,false))
      oos.writeObject(instance.getDeepLearningParams)
    }
  }


  private class H2ODeepLearningReader extends MLReader[H2ODeepLearning] {

    private val className = classOf[H2ODeepLearning].getName

    override def load(path: String): H2ODeepLearning = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val file = new File(path,paramsFileName)
      val ois = new ObjectInputStream(new FileInputStream(file))
      val deepLearningParameters = ois.readObject().asInstanceOf[DeepLearningParameters]
      implicit val h2OContext = H2OContext.getOrCreate(sc)
      implicit val sQLContext = SQLContext.getOrCreate(sc)
      val h2oDeepLearning = new H2ODeepLearning(deepLearningParameters,metadata.uid)
      DefaultParamsReader.getAndSetParams(h2oDeepLearning, metadata)
      h2oDeepLearning
    }
  }

  @Since("1.6.0")
  override def read: MLReader[H2ODeepLearning] = new H2ODeepLearningReader

  @Since("1.6.0")
  override def load(path: String): H2ODeepLearning = super.load(path)
}
/**
  * Parameters here can be set as normal and are duplicated to DeepLearningParameters H2O object
  */
trait DeepLearningParams extends Params {

  /**
    * Holder of the parameters, we use it so set default values and then to store the set values by the user
    */
  private var deepLearningParameters = new DeepLearningParameters()
  protected def getDeepLearningParams: DeepLearningParameters = deepLearningParameters
  protected def setDeepLearningParams( deepLearningParameters: DeepLearningParameters) = this.deepLearningParameters = deepLearningParameters
  /**
    * All parameters should be set here along with their documentation and explained default values
    */
  final val epochs = new DoubleParam(this, "epochs", "Explanation")
  final val l1 = new DoubleParam(this, "l1", "Explanation")
  final val l2 = new DoubleParam(this, "l2", "Explanation")
  final val hidden = new IntArrayParam(this, "hidden", "Explanation")
  final val responseColumn = new Param[String](this,"responseColumn","Explanation")
  final val validKey = new H2OKeyParam[Frame](this,"valid","Explanation")
  final val trainKey = new H2OKeyParam[Frame](this,"train","Explanation")

  setDefault(epochs->deepLearningParameters._epochs)
  setDefault(l1->deepLearningParameters._l1)
  setDefault(l2->deepLearningParameters._l2)
  setDefault(hidden->deepLearningParameters._hidden)
  setDefault(responseColumn->deepLearningParameters._response_column)
  setDefault(validKey->deepLearningParameters._valid)
  setDefault(trainKey->deepLearningParameters._train)

  /** @group getParam */
  def getEpochs: Double = $(epochs)
  /** @group getParam */
  def getL1: Double = $(l1)
  /** @group getParam */
  def getL2: Double = $(l2)
  /** @group getParam */
  def getHidden: Array[Int] = $(hidden)
  /** @group getParam */
  def getResponseColumn: String = $(responseColumn)
  /** @group getParam */
  def getValidKey: String = $(validKey).toString
  /** @group getParam */
  def getTrainKey: String = $(trainKey).toString
}

