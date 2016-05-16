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

package org.apache.spark.examples.h2o

import org.apache.spark.examples.h2o.HamOrSpamDemo._
import org.apache.spark.{SparkConf, SparkContext}
import water.app.SparkContextSupport

object SparkModelExport extends SparkContextSupport  {

  val DATAFILE="smsData.txt"

  def main(args: Array[String]) {
    val conf: SparkConf = configure("Spark TF and IDF model export example.")
    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    // Register input file as Spark file
    addFiles(sc, absPath("examples/smalldata/" + DATAFILE))

    // Data load
    val data = load(sc, DATAFILE)
    // Extract response spam or ham
    val message = data.map( r => r(1))
    // Tokenize message content
    val tokens = tokenize(message)

    // Build IDF model
    val (hashingTF, idfModel, _) = buildIDFModel(tokens)

    // Implicit converters
    import org.apache.spark.model.SparklingModel._

    // Save both models as POJO
    hashingTF.save()
    idfModel.save()

    // Perform TF and IDF using spark models and save them to compare them later with the POJO model version
    tokens.map { msg =>
      val tf = hashingTF.transform(msg)
      val tfidf = idfModel.transform(tf)
      s"${msg.mkString(" ")}\t$tf\t$tfidf"
    }.saveAsTextFile("spark-tfidf-data")

  }

}
