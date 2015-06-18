package org.apache.flink.streaming.incrementalML.sampling

import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.classification.HoeffdingTree
import org.apache.flink.streaming.incrementalML.evaluator.PrequentialEvaluator
import org.apache.flink.streaming.sampling.helpers.{Configuration, SamplingUtils}
import org.apache.flink.streaming.sampling.samplers._

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

/**
 * Created by marthavk on 2015-06-17.
 */
object SampledClassification {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************


  def main(args: Array[String]) {

/*    if (!parseParameters(args)) {
      return
    }*/

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // read properties
    val initProps = SamplingUtils.readProperties(SamplingUtils.path
      + "distributionconfig.properties")

    val file = "/home/marthavk/Desktop/thesis-all-docs/resources/dataSets/randomRBF/randomRBF-10M.arff"
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

    /*val reservoirSampler1000: SampleFunction[LabeledVector] =
    new UniformSampler[LabeledVector](Configuration.SAMPLE_SIZE_1000, 100)*/

    val fifoSampler1000: SampleFunction[LabeledVector] =
    new FiFoSampler[LabeledVector](Configuration.SAMPLE_SIZE_1000, 100)

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




    val sampler: StreamSampler[LabeledVector] =
      new StreamSampler[LabeledVector](fifoSampler1000)

    dataPoints.getJavaStream.transform("sample", dataPoints.getType, sampler)

    val vfdTree = HoeffdingTree(env)
    val evaluator = PrequentialEvaluator()

    val streamToEvaluate = vfdTree.fit(dataPoints, parameters)

    evaluator.evaluate(streamToEvaluate).writeAsText("/home/marthavk/Desktop/thesis-all-docs/results/classification_results/" + "rbf_FS1K1000")
      .setParallelism(1)

    env.execute()
  }


  // *************************************************************************
  // UTIL METHODS
  // *************************************************************************

  private var fileInput: Boolean = false
  private var fileOutput: Boolean = false

  private var inputPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if(args.length == 1) {
      fileInput = true
      inputPath = args(0)
    }
    else if (args.length == 2) {
      fileInput = true
      fileOutput = true
      inputPath = args(0)
      outputPath = args(1)
    }
    else {
      System.err.println("Usage: SampledClassification <input path> or SampledClassification <input path> " +
        "<output path>")
      return false
    }
    return true
  }

}
