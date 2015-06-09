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
import org.apache.flink.streaming.sampling.generators.DoubleDataGenerator;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.helpers.Configuration;
import org.apache.flink.streaming.sampling.samplers.StreamSampler;
import org.apache.flink.streaming.sampling.samplers.UniformSampler;
import org.apache.flink.streaming.sampling.sources.NormalStreamSource;

/**
 * Created by marthavk on 2015-05-06.
 */
public class ReservoirSamplingExample {

	public static String outputPath;
	public static int sample_size;

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	/**
	 * Sample the Stream Using Biased Reservoir Sampling with different buffer sizes: 1000,5000,10000,50000
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		/*create debug source*/
		//DataStreamSource<Long> debugSource = env.addSource(new DebugSource(500000));

		/** OR **/

		/*create stream of distributions as source (also number generators) and shuffle*/
		DataStreamSource<GaussianDistribution> source = createSource(env);
		//SingleOutputStreamOperator<GaussianDistribution, ?> shuffledSrc = source.shuffle();

		/*generate random number from distribution*/
		SingleOutputStreamOperator<Double, ?> doubleStream =
				source.map(new DoubleDataGenerator<GaussianDistribution>());


		/*create samplerS*/
		UniformSampler<Double> uniformSampler1000 = new UniformSampler<Double>(sample_size, 100);
		//UniformSampler<Double> uniformSampler5000 = new UniformSampler<Double>(Configuration.SAMPLE_SIZE_5000, 100);
		//UniformSampler<Double> uniformSampler10000 = new UniformSampler<Double>(Configuration.SAMPLE_SIZE_10000, 100);
		//UniformSampler<Double> uniformSampler50000 = new UniformSampler<Double>(Configuration.SAMPLE_SIZE_50000, 100);

		/*sample*/
		doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(uniformSampler1000));
		//doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(uniformSampler5000));
		//doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(uniformSampler10000));
		//doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(uniformSampler50000));

		/*get js for execution plan*/
		System.err.println(env.getExecutionPlan());

		/*execute program*/
		env.execute("Reservoir Sampling Experiment");

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

	private static boolean parseParameters(String[] args) {
		if (args.length == 1) {
			outputPath = args[0];
			return true;
		} else if (args.length == 0) {
			outputPath = "";
			return true;
		} else {
			System.err.println("Usage: ReservoirSamplingExample <size> <path>");
			return false;
		}
	}
}
