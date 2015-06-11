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
import org.apache.flink.streaming.sampling.samplers.BiasedReservoirSampler;
import org.apache.flink.streaming.sampling.samplers.ChainSampler;
import org.apache.flink.streaming.sampling.samplers.FiFoSampler;
import org.apache.flink.streaming.sampling.samplers.PrioritySampler;
import org.apache.flink.streaming.sampling.samplers.StreamSampler;
import org.apache.flink.streaming.sampling.samplers.UniformSampler;
import org.apache.flink.streaming.sampling.sources.NormalStreamSource;

/**
 * Created by marthavk on 2015-05-08.
 */
public class SamplingExample {

	public static int sample_size;

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		/*create debug source*/
		//DataStreamSource<Long> debugSource = env.addSource(new DebugSource(500000));

		/*create stream of distributions as source (also number generators) and shuffle*/
		DataStreamSource<GaussianDistribution> source = createSource(env);
		//SingleOutputStreamOperator<GaussianDistribution, ?> shuffledSrc = source.shuffle();

		/*generate random number from distribution*/
		SingleOutputStreamOperator<Double, ?> doubleStream =
				source.map(new DoubleDataGenerator<GaussianDistribution>());

		int sizeInKs = sample_size/1000;

		/** BIASED **/
		BiasedReservoirSampler<Double> biasedReservoirSampler1000 = new BiasedReservoirSampler<Double>(sample_size, 100);
		doubleStream.transform("sampleBRS"+sizeInKs+"K", doubleStream.getType(), new StreamSampler<Double>(biasedReservoirSampler1000));

		/** CHAIN **/
		ChainSampler<Double> chainSampler1000 = new ChainSampler<Double>(sample_size, Configuration.countWindowSize, 100);
		doubleStream.transform("sampleCS" + sizeInKs + "K", doubleStream.getType(), new StreamSampler<Double>(chainSampler1000));

		/** FIFO **/
		FiFoSampler<Double> fifoSampler1000 = new FiFoSampler<Double>(sample_size, 100);
		doubleStream.transform("sampleFS" + sizeInKs + "K" , doubleStream.getType(), new StreamSampler<Double>(fifoSampler1000));

		/** PRIORITY **/
		PrioritySampler<Double> prioritySampler1000 = new PrioritySampler<Double>(sample_size, Configuration.timeWindowSize, 100);
		doubleStream.transform("samplePS" + sizeInKs + "K", doubleStream.getType(), new StreamSampler<Double>(prioritySampler1000));

		/** UNIFORM SAMPLER **/
		UniformSampler<Double> uniformSampler1000 = new UniformSampler<Double>(sample_size, 100);
		doubleStream.transform("sampleRS" + sizeInKs + "K", doubleStream.getType(), new StreamSampler<Double>(uniformSampler1000));

		/*get js for execution plan*/
		System.err.println(env.getExecutionPlan());

		/*execute program*/
		env.execute("Sampling Experiment");

	}

	/**
	 * Creates a DataStreamSource of GaussianDistribution items out of the params at input.
	 *
	 * @param env the StreamExecutionEnvironment.
	 * @return the DataStreamSource
	 */
	public static DataStreamSource<GaussianDistribution> createSource(StreamExecutionEnvironment env) {
		System.out.println("--- sample size: " + sample_size);
		System.out.println("--- output path: " + Configuration.outputPath);
		return env.addSource(new NormalStreamSource());
	}

	private static boolean parseParameters(String[] args) {
		if (args.length == 2) {
			sample_size = Integer.parseInt(args[0]);
			Configuration.outputPath = args[1];
			return true;
		} else {
			System.err.println("Usage: SamplingExample <size> <path>");
			return false;
		}
	}


}
