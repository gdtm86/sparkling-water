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
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import water.util.SBPrintStream

class SparklingIDFModelTest extends FunSuite {

  test("Should generate spark agnostic IDF model") {
    val conf: SparkConf = new SparkConf()
      .setAppName("Spark model test")
      .setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local[*]"))
    val sc = new SparkContext(conf)

    import org.apache.spark.model.SparklingModel._
    val baos = new ByteArrayOutputStream()
    val hashingTF = new HashingTF(5)

    val data = sc.parallelize(Array("I love pie".split(" ").toSeq))
    val tf = hashingTF.transform(data)
    val idfModel = new IDF().fit(tf)

    idfModel.toJava(new SBPrintStream(baos))
    val generated = new String(baos.toByteArray, Charsets.UTF_8)
    println(generated)
    assert(generated.equals(
      """import water.*;
        |
        |public class SparklingIDFModel {
        |
        |  private Vector idf = new DenseVector(new double[] {0.6931471805599453,0.6931471805599453,0.6931471805599453,0.0,0.6931471805599453});
        |
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
        |}
        |""".stripMargin
    ))
  }

}
