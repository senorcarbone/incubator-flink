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

package org.apache.flink.streaming.sampling.examples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.sampling.evaluators.NormalAggregator;
import org.apache.flink.streaming.sampling.generators.DoubleDataGenerator;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.samplers.StreamSampler;
import org.apache.flink.streaming.sampling.samplers.UniformSampler;
import org.apache.flink.streaming.sampling.sources.DebugSource;
import org.apache.flink.streaming.sampling.sources.NormalStreamSource;

/**
 * Created by marthavk on 2015-03-13.
 */
public class StreamApproximationExample {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	public static void main(String[] args) throws Exception {

		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		/*create debug source*/
		//DataStreamSource<Long> debugSource = env.addSource(new DebugSource(500000));

		/*create stream of distributions as source (also number generators) and shuffle*/
		DataStreamSource<Long> source = env.addSource(new DebugSource(100000));
		//SingleOutputStreamOperator<GaussianDistribution, ?> shuffledSrc = source.shuffle();

		/*generate random number from distribution*//*
		SingleOutputStreamOperator<Double, ?> doubleStream =
				source.map(new DoubleDataGenerator<GaussianDistribution>());*/
		int sample_size = 1000;
		int output_rate = 2560000;
		int sizeInKs = sample_size / 1000;


		/** UNIFORM SAMPLER **/
		UniformSampler<Long> uniformSampler1000 = new UniformSampler<Long>(sample_size, output_rate);
		SingleOutputStreamOperator<Long, ?> dataStream = source.transform("sampleRS" + sizeInKs + "K", source.getType(), new StreamSampler<Long>(uniformSampler1000));
		dataStream.count().print();
		/*get js for execution plan*/
		System.err.println(env.getExecutionPlan());

		/*execute program*/
		env.execute("Sampling Experiment");

	}


	/**
	 * Evaluates the sampling method. Compares final sample distribution parameters
	 * with source.
	 *
	 * @param env
	 */
	public static void evaluateSampling(StreamExecutionEnvironment env) {


		/*create stream of distributions as source (also number generators) and shuffle*/
		DataStreamSource<GaussianDistribution> source = createSource(env);
		SingleOutputStreamOperator<GaussianDistribution, ?> shuffledSrc = source.shuffle();

		/*generate random number from distribution*/
		SingleOutputStreamOperator<Double, ?> generator = shuffledSrc.map(new DoubleDataGenerator<GaussianDistribution>());

		SingleOutputStreamOperator<GaussianDistribution, ?> aggregator = generator.map(new NormalAggregator());

		//// SingleOutputStreamOperator<Sample<Double>, ?> sample = generator.map(new MetaAppender<Double>())

				/*sample the stream*/
		////	.map(new ReservoirSampler<Tuple3<Double, StreamTimestamp,Long>>(sampleSize))

				/*extract Double sampled values (unwrap from Tuple3)*/
		//.map(new SampleExtractor<Double>()); //use that for Chain and Priority Samplers.
		////	.map(new SimpleUnwrapper<Double>()); //use that for Reservoir, Biased Reservoir, FIFO Samplers

		/*connect sampled stream to source*/
		//// sample.connect(aggregator)

				/*evaluate sample: compare current distribution parameters with sampled distribution parameters*/
		////		.flatMap(new DistanceEvaluator())
		//.flatMap(new DistributionComparator())
		//.setParallelism(1)

				/*sink*/
		////		.sum(0)
		////		.writeAsText(SamplingUtils.path + "evaluation");
		//.setParallelism(1);
	}


	/**
	 * Creates a DataStreamSource of GaussianDistribution items out of the params at input.
	 *
	 * @param env the StreamExecutionEnvironment.
	 * @return the DataStreamSource
	 */
	public static DataStreamSource<GaussianDistribution> createSource(StreamExecutionEnvironment env) {
		return env.addSource(new NormalStreamSource());
	}


}
