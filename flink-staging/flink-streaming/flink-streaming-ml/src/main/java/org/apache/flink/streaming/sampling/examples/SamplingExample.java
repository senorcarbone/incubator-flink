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
import org.apache.flink.streaming.sampling.samplers.*;
import org.apache.flink.streaming.sampling.sources.NormalStreamSource;

/**
 * Created by marthavk on 2015-05-08.
 */
public class SamplingExample {
	public static String outputPath;

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

		/** BIASED **/
		/*create samplerS*/
		BiasedReservoirSampler<Double> biasedReservoirSampler1000 = new BiasedReservoirSampler<Double>(Configuration.SAMPLE_SIZE_1000, 100);
		BiasedReservoirSampler<Double> biasedReservoirSampler5000 = new BiasedReservoirSampler<Double>(Configuration.SAMPLE_SIZE_5000, 100);
		BiasedReservoirSampler<Double> biasedReservoirSampler10000 = new BiasedReservoirSampler<Double>(Configuration.SAMPLE_SIZE_10000, 100);
		BiasedReservoirSampler<Double> biasedReservoirSampler50000 = new BiasedReservoirSampler<Double>(Configuration.SAMPLE_SIZE_50000, 100);

		/*sample*/
		doubleStream.transform("sampleB1K", doubleStream.getType(), new StreamSampler<Double>(biasedReservoirSampler1000));
		doubleStream.transform("sampleB5K", doubleStream.getType(), new StreamSampler<Double>(biasedReservoirSampler5000));
		doubleStream.transform("sampleB10K", doubleStream.getType(), new StreamSampler<Double>(biasedReservoirSampler10000));
		doubleStream.transform("sampleB50K", doubleStream.getType(), new StreamSampler<Double>(biasedReservoirSampler50000));

		/** CHAIN **/
		/*create samplerS*/
		ChainSampler<Double> chainSampler1000 = new ChainSampler<Double>(Configuration.SAMPLE_SIZE_1000, Configuration.countWindowSize, 100);
		ChainSampler<Double> chainSampler5000 = new ChainSampler<Double>(Configuration.SAMPLE_SIZE_5000, Configuration.countWindowSize, 100);
		ChainSampler<Double> chainSampler10000 = new ChainSampler<Double>(Configuration.SAMPLE_SIZE_10000, Configuration.countWindowSize, 100);
		ChainSampler<Double> chainSampler50000 = new ChainSampler<Double>(Configuration.SAMPLE_SIZE_50000, Configuration.countWindowSize, 100);

		/*sample*/
		doubleStream.transform("sampleC1K", doubleStream.getType(), new StreamSampler<Double>(chainSampler1000));
		doubleStream.transform("sampleC5K", doubleStream.getType(), new StreamSampler<Double>(chainSampler5000));
		doubleStream.transform("sampleC10K", doubleStream.getType(), new StreamSampler<Double>(chainSampler10000));
		doubleStream.transform("sampleC50K", doubleStream.getType(), new StreamSampler<Double>(chainSampler50000));

		/** FIFO **/
		/*create samplerS*/
		FiFoSampler<Double> fifoSampler1000 = new FiFoSampler<Double>(Configuration.SAMPLE_SIZE_1000, 100);
		FiFoSampler<Double> fifoSampler5000 = new FiFoSampler<Double>(Configuration.SAMPLE_SIZE_5000, 100);
		FiFoSampler<Double> fifoSampler10000 = new FiFoSampler<Double>(Configuration.SAMPLE_SIZE_10000, 100);
		FiFoSampler<Double> fifoSampler50000 = new FiFoSampler<Double>(Configuration.SAMPLE_SIZE_50000, 100);

		/*sample*/
		doubleStream.transform("sampleF1K", doubleStream.getType(), new StreamSampler<Double>(fifoSampler1000));
		doubleStream.transform("sampleF5K", doubleStream.getType(), new StreamSampler<Double>(fifoSampler5000));
		doubleStream.transform("sampleF10F", doubleStream.getType(), new StreamSampler<Double>(fifoSampler10000));
		doubleStream.transform("sampleF50K", doubleStream.getType(), new StreamSampler<Double>(fifoSampler50000));

		/** PRIORITY **/
		/*create samplerS*/
		PrioritySampler<Double> prioritySampler1000 = new PrioritySampler<Double>(Configuration.SAMPLE_SIZE_1000, Configuration.timeWindowSize, 100);
		PrioritySampler<Double> prioritySampler5000 = new PrioritySampler<Double>(Configuration.SAMPLE_SIZE_5000, Configuration.timeWindowSize, 100);
		PrioritySampler<Double> prioritySampler10000 = new PrioritySampler<Double>(Configuration.SAMPLE_SIZE_10000, Configuration.timeWindowSize, 100);
		PrioritySampler<Double> prioritySampler50000 = new PrioritySampler<Double>(Configuration.SAMPLE_SIZE_50000, Configuration.timeWindowSize, 100);

		/*sample*/
		doubleStream.transform("sampleP1K", doubleStream.getType(), new StreamSampler<Double>(prioritySampler1000));
		doubleStream.transform("sampleP5K", doubleStream.getType(), new StreamSampler<Double>(prioritySampler5000));
		doubleStream.transform("sampleP10K", doubleStream.getType(), new StreamSampler<Double>(prioritySampler10000));
		doubleStream.transform("sampleP50K", doubleStream.getType(), new StreamSampler<Double>(prioritySampler50000));

		/** UNIFORM SAMPLER **/
		/*create samplerS*/
		UniformSampler<Double> uniformSampler1000 = new UniformSampler<Double>(Configuration.SAMPLE_SIZE_1000, 100);
		UniformSampler<Double> uniformSampler5000 = new UniformSampler<Double>(Configuration.SAMPLE_SIZE_5000, 100);
		UniformSampler<Double> uniformSampler10000 = new UniformSampler<Double>(Configuration.SAMPLE_SIZE_10000, 100);
		UniformSampler<Double> uniformSampler50000 = new UniformSampler<Double>(Configuration.SAMPLE_SIZE_50000, 100);

		/*sample*/
		doubleStream.transform("sampleU1K", doubleStream.getType(), new StreamSampler<Double>(uniformSampler1000));
		doubleStream.transform("sampleU5K", doubleStream.getType(), new StreamSampler<Double>(uniformSampler5000));
		doubleStream.transform("sampleU10K", doubleStream.getType(), new StreamSampler<Double>(uniformSampler10000));
		doubleStream.transform("sampleU50K", doubleStream.getType(), new StreamSampler<Double>(uniformSampler50000));

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
			System.err.println("Usage: ReservoirSamplingExample <path>");
			return false;
		}
	}


}
