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

package org.apache.spark.model

import java.io.FileOutputStream

import org.apache.spark.mllib.feature.{HashingTF, IDFModel}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import water.codegen.CodeGeneratorPipeline
import water.util.SBPrintStream

import scala.language.implicitConversions

object SparklingModel {

  implicit def sparkToSparkling(model: IDFModel): SparklingIDFModel = new SparklingIDFModel(model)

  implicit def sparkToSparkling(model: HashingTF): SparklingTFModel = new SparklingTFModel(model)

}

trait SparklingModel {

  val name: String

  def toJava(sb: SBPrintStream): SBPrintStream

  def save(): Unit = save(s"$name.java")

  def save(file: String): Unit = toJava(new SBPrintStream(new FileOutputStream(file)))
}

class SparklingIDFModel(val sparkModel: IDFModel) extends SparklingModel {

  val name = "SparklingIDFModel"

  def toJava(sb: SBPrintStream): SBPrintStream = {

    val fileCtx: CodeGeneratorPipeline = new CodeGeneratorPipeline

    sb.p("import water.*;").nl

    sb.p("public class ").p(name).p(" {").nl.ii(1)

    sb.nl()

    sparkModel.idf match {
      case DenseVector(_) =>
        sb.p("  private Vector idf = ").p(s"new DenseVector(${newDoubleArrString(sparkModel.idf.toArray)});").nl()
      case SparseVector(_) => {
        val sparse = sparkModel.idf.asInstanceOf[SparseVector]
        sb.p("  private Vector idf = ")
          .p(s"""new SparseVector(
                |${sparse.size},
                |${newIntArrString(sparse.indices)},
                |${newDoubleArrString(sparse.values)}
                |);
                |""".stripMargin
          )
          .nl()
      }
    }

    // TODO should this work by overriding v instead?
    sb.p(
      """
        |  public Vector transform(Vector v) {
        |    int n = v.size();
        |    for(int i = 0; i < n; i++) {
        |      v.set(i, v.get(i) * idf.get(i));
        |    }
        |    return v;
        |  }
      """.stripMargin).nl()

    sb.p("}").nl.di(1)
    fileCtx.generate(sb)
    sb.nl
    sb
  }

  private def newIntArrString(arr: Array[Int]) =
    s"""new int[] {${arr.mkString(",")}}"""

  private def newDoubleArrString(arr: Array[Double]) =
    s"""new double[] {${arr.mkString(",")}}"""

}

class SparklingTFModel(val sparkModel: HashingTF) extends SparklingModel {

  val name = "SparklingTFModel"

  def toJava(sb: SBPrintStream): SBPrintStream = {

    val fileCtx: CodeGeneratorPipeline = new CodeGeneratorPipeline

    sb.p(
      """import java.util.HashMap;
        |import java.util.Map;
        |import water.*;
      """.stripMargin).nl

    sb.p("public class ").p(name).p(" {").nl.ii(1)

    sb.nl()

    sb.p("  private int numFeatures = ").p(s"${sparkModel.numFeatures};").nl()

    // TODO move indexOf/nonNegativeMod to util jar?
    sb.p(
      """
        |  public Vector transform(Iterable<?> document) {
        |    Map<Integer, Double> termFrequencies = new HashMap<>();
        |    for(Object term : document) {
        |      int i = indexOf(term);
        |      termFrequencies.put(i, termFrequencies.getOrDefault(i, 0.0) + 1.0);
        |    }
        |    return new SparseVector(termFrequencies);
        |  }
        |
        |  private Integer indexOf(Object term) { return nonNegativeMod(term.hashCode(), numFeatures); }
        |
        |  private Integer nonNegativeMod(int x, int mod) {
        |    int rawMod = x % mod;
        |    return rawMod + (rawMod < 0 ? mod : 0);
        |  }
      """.stripMargin).nl()

    sb.p("}").nl.di(1)
    fileCtx.generate(sb)
    sb.nl
    sb
  }

}
