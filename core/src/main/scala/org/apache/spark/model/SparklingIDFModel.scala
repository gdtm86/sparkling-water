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

    /**/ sb.p("import water.*;").nl.nl
    /**/
    /**/ sb.p(s"public class $name {").nl.nl
    /**/
    sparkModel.idf match {
      case DenseVector(_) =>
        /*  */ sb.i(1).p(s"private Vector idf = new DenseVector(${newDoubleArrString(sparkModel.idf.toArray)});").nl.nl
      case SparseVector(_) => {
        val sparse = sparkModel.idf.asInstanceOf[SparseVector]
        /*  */ sb.i(1).p("private Vector idf = new SparseVector(").nl
        /*  */ sb.i(2).p(s"${sparse.size},").nl
        /*  */ sb.i(2).p(s"${newIntArrString(sparse.indices)},").nl
        /*  */ sb.i(2).p(s"${newDoubleArrString(sparse.values)}").nl
        /*  */ sb.i(1).p(");").nl.nl
      }
    }
    /**/
    /*  */ sb.i(1).p("public Vector transform(Vector inp) {").nl
    /*    */ sb.i(2).p("Vector v = inp.clone();").nl
    /*    */ sb.i(2).p("if(v.isSparse()) {").nl
    /*      */ sb.i(3).p("for(int i : inp.indices()) {").nl
    /*        */ sb.i(4).p("v.set(i, v.get(i) * idf.get(i));").nl
    /*      */ sb.i(3).p("}").nl
    /*    */ sb.i(2).p("} else {").nl
    /*      */ sb.i(3).p("for(int i = 0; i < inp.size(); i++) {").nl
    /*        */ sb.i(4).p("v.set(i, v.get(i) * idf.get(i));").nl
    /*      */ sb.i(3).p("}").nl
    /*    */ sb.i(2).p("}").nl
    /*    */ sb.i(2).p("return v;").nl
    /*  */ sb.i(1).p("}").nl
    /**/ sb.p("}").nl
    fileCtx.generate(sb)
    sb
  }

  private def newIntArrString(arr: Array[Int]) =
    s"""new int[] {${arr.mkString(",")}}"""

  private def newDoubleArrString(arr: Array[Double]) =
    s"""new double[] {${arr.mkString(",")}}"""

}
