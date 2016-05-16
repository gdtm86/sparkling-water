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

import org.apache.spark.mllib.feature.HashingTF
import water.codegen.CodeGeneratorPipeline
import water.util.SBPrintStream

class SparklingTFModel(val sparkModel: HashingTF,
                       val name: String = "SparklingTFModel") extends SparklingModel {

  def toJava(sb: SBPrintStream): SBPrintStream = {

    val fileCtx: CodeGeneratorPipeline = new CodeGeneratorPipeline

    /**/ sb.p("import java.util.HashMap;").nl
    /**/ sb.p("import java.util.Map;").nl
    /**/ sb.p("import water.*;").nl.nl
    /**/
    /**/ sb.p("public class ").p(name).p(" {").nl
    /*  */ sb.i(1).p(s"private int numFeatures = ${sparkModel.numFeatures};").nl.nl
    /**/
    /*  */ sb.i(1).p("public Vector transform(Iterable<?> document) {").nl
    /*    */ sb.i(2).p("Map<Integer, Double> termFrequencies = new HashMap<>();").nl
    /*    */ sb.i(2).p("for(Object term : document) {").nl
    /*      */ sb.i(3).p("int i = indexOf(term);").nl
    /*      */ sb.i(3).p("termFrequencies.put(i, termFrequencies.getOrDefault(i, 0.0) + 1.0);").nl
    /*    */ sb.i(2).p("}").nl
    /*    */ sb.i(2).p("return new SparseVector(termFrequencies);").nl
    /*  */ sb.i(1).p("}").nl.nl
    /**/
    /*  */ sb.i(1).p("private Integer indexOf(Object term) { return nonNegativeMod(term.hashCode(), numFeatures); }").nl.nl
    /**/
    /*  */ sb.i(1).p("private Integer nonNegativeMod(int x, int mod) {").nl
    /*    */ sb.i(2).p("int rawMod = x % mod;").nl
    /*    */ sb.i(2).p("return rawMod + (rawMod < 0 ? mod : 0);").nl
    /*  */ sb.i(1).p("}").nl
    /**/ sb.p("}").nl
    fileCtx.generate(sb)
    sb
  }

}
