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

import org.apache.spark.mllib.feature.IDFModel
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import water.codegen.CodeGeneratorPipeline
import water.util.SBPrintStream

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

    sb.p(
      """
        |  public Vector transform(Vector inp) {
        |    Vector v = inp.clone();
        |    if(v.isSparse()) {
        |      for(int i : inp.indices()) {
        |        v.set(i, v.get(i) * idf.get(i));
        |      }
        |    } else {
        |      for(int i = 0; i < inp.size(); i++) {
        |        v.set(i, v.get(i) * idf.get(i));
        |      }
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
