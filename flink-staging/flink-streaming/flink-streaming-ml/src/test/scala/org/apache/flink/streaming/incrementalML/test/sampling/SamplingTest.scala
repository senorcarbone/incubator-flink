/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.incrementalML.test.sampling

import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.classification.HoeffdingTree
import org.apache.flink.streaming.incrementalML.evaluator.PrequentialEvaluator
import org.apache.flink.streaming.sampling.helpers.{Configuration, SamplingUtils}
import org.apache.flink.streaming.sampling.samplers.{UniformSampler, BiasedReservoirSampler, SampleFunction, StreamSampler}
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}


/**
 * Created by marthavk on 2015-06-01.
 */
class SamplingTest
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Very Fast Decision Tree algorithm"

  it should "Create the classification HT of the given data set" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // read properties
    val initProps = SamplingUtils.readProperties(Configuration.path
      + "distributionconfig.properties")

    //val file = "/home/marthavk/Desktop/thesis-all-docs/resources/dataSets/randomRBF/randomRBF-10M.arff"
    //val max_count = initProps.getProperty("maxCount").toInt
    val sample_size = initProps.getProperty("sampleSize").toInt

    //set parameters of VFDT
    val parameters = ParameterMap()
    //    val nominalAttributes = Map(0 ->4, 2 ->4, 4 ->4, 6 ->4 8 ->4)
    parameters.add(HoeffdingTree.MinNumberOfInstances, 300)
    parameters.add(HoeffdingTree.NumberOfClasses, 4)
    parameters.add(HoeffdingTree.Parallelism, 4)

    //read datapoints for covertype_libSVM dataset and sample

    //val sample = StreamingMLUtils.readLibSVM(env, SamplingUtils.covertypePath, 54)
    /*val biasedReservoirSampler1000: SampleFunction[LabeledVector] =
      new BiasedReservoirSampler[LabeledVector](Configuration.SAMPLE_SIZE_1000, 100)*/

    val uniformSampler1000: SampleFunction[LabeledVector] =
      new UniformSampler[LabeledVector](Configuration.SAMPLE_SIZE_1000, 100)

    // read datapoints for randomRBF dataset
    val dataPoints = env.readTextFile("/home/marthavk/Desktop/thesis-all-docs/resources/dataSets/randomRBF/randomRBF-10M.arff").map {
      line => {
        var featureList = Vector[Double]()
        val features = line.split(',')
        for (i <- 0 until features.size - 1) {
          featureList = featureList :+ features(i).trim.toDouble
        }

        LabeledVector(features(features.size - 1).trim.toDouble, DenseVector(featureList.toArray))
      }
    }

    //create sampler
    val sampler: StreamSampler[LabeledVector] =
      new StreamSampler[LabeledVector](uniformSampler1000)

    //sample datapoints
    dataPoints.getJavaStream.transform("sample", dataPoints.getType, sampler)

    //create vfdTree
    val vfdTree = HoeffdingTree(env)

    //create evaluator
    val evaluator = PrequentialEvaluator()
    val streamToEvaluate = vfdTree.fit(dataPoints, parameters)

    //evaluate sampled datapoints
    evaluator.evaluate(streamToEvaluate).writeAsText("/home/marthavk/Desktop/thesis-all-docs/results/classification_results/" + "rbf_RS1K100")
      .setParallelism(1)

    env.execute()

  }

}
