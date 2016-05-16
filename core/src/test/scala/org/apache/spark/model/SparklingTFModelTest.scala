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

import java.io.ByteArrayOutputStream

import com.google.common.base.Charsets
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import water.util.SBPrintStream

class SparklingTFModelTest extends FunSuite {

  test("Should generate spark agnostic TF model") {
    val conf: SparkConf = new SparkConf()
      .setAppName("Spark model test")
      .setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local[*]"))
    val sc = new SparkContext(conf)

    import org.apache.spark.model.SparklingModel._
    val baos = new ByteArrayOutputStream()
    new HashingTF(1024).toJava(new SBPrintStream(baos))
    val generated = new String(baos.toByteArray, Charsets.UTF_8)

    assert(generated.equals(
      """import java.util.HashMap;
        |import java.util.Map;
        |import water.*;
        |
        |public class SparklingTFModel {
        |  private int numFeatures = 1024;
        |
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
        |}
        |""".stripMargin
    ))
  }

}
