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

package org.apache.flink.streaming.examples.sampling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.function.sink.RichSinkFunction;
import org.apache.flink.streaming.api.function.source.RichSourceFunction;
import org.apache.flink.streaming.examples.sampling.generators.Evaluator;
import org.apache.flink.streaming.examples.sampling.generators.GaussianGenerator;
import org.apache.flink.streaming.examples.sampling.samplers.Reservoir;
import org.apache.flink.util.Collector;

/**
 * Created by marthavk on 2015-03-13.
 */
public class StreamApproximationExample {

	public static long MAX_COUNT = 100000;
	//set sample size
	public int rSize = 100;

	//set parameters for random evolving gaussian sample generator
	public double meanInit = 0;
	public double sigmaInit = 100;
	public double mstep = 1;
	public double sstep = -0.05;
	public int interval = 100;
	public Parameters initParams = new Parameters(meanInit, sigmaInit, mstep, sstep, interval, rSize);

	public StreamApproximationExample() throws Exception {

		//set execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		evaluateSampling(env, initParams);
		env.execute();
	} //end of StreamApproximationExample cTor


	public static void main(String args[]) throws Exception {
		new StreamApproximationExample();
	}


	public static void evaluateSampling(StreamExecutionEnvironment env, final Parameters params) {
		//create source
		DataStreamSource<GaussianGenerator> source = createSource(env, params);
		source.map(new MapFunction<GaussianGenerator,Double>() {
			@Override
			public Double map(GaussianGenerator generator) throws Exception {
				return generator.generate();
			}
		}).map(new RichMapFunction<Double,Reservoir<Double>>() {
			Reservoir<Double> reservoir = new Reservoir<Double>(params.getSampleSize());
			int count = 0;

			@Override
			public Reservoir<Double> map(Double value) throws Exception {
				reservoir.sample(value);
				return reservoir;
			}
		})
				.connect(source)
				.flatMap(new CoFlatMapFunction<Reservoir<Double>,GaussianGenerator,Double>() {

					GaussianGenerator currentDist = new GaussianGenerator();

					@Override
					public void flatMap1(Reservoir<Double> value, Collector<Double> out) throws Exception {
						GaussianGenerator sampledDist = new GaussianGenerator(value);
						//System.out.println(currentDist.toString() + " " + sampledDist.toString());
						out.collect(Evaluator.evaluate(currentDist, sampledDist));
					}

					@Override
					public void flatMap2(GaussianGenerator value, Collector<Double> out) throws Exception {
						currentDist = value;
					}
				})
				.addSink(new RichSinkFunction<Double>() {
					@Override
					public void invoke(Double value) throws Exception {
						RuntimeContext context = getRuntimeContext();
						if (context.getIndexOfThisSubtask() == 0) {
							System.out.println(value);
						}
					}

					@Override
					public void cancel() {

					}
				});

	}

	/**
	 * Creates a DataStreamSource of Distribution items out of the params at input.
	 *
	 * @param env the StreamExecutionEnvironment.
	 * @return the DataStreamSource
	 */
	public static DataStreamSource<GaussianGenerator> createSource(StreamExecutionEnvironment env, final Parameters params) {
		return env.addSource(new RichSourceFunction<GaussianGenerator>() {

			long count = 0;
			GaussianGenerator gaussD = new GaussianGenerator(params.getMeanInit(), params.getSigmaInit());

			@Override
			public void run(Collector<GaussianGenerator> collector) throws Exception {

				while (count < MAX_COUNT) {
					count++;
					gaussD.updateMean(count, params.getmStep(), params.getInterval());
					gaussD.updateSigma(count, params.getsStep(), params.getInterval());
					//double newItem = Gaussian.generate(gaussD.getMean(), gaussD.getSigma());

					collector.collect(gaussD);
				}
			}

			@Override
			public void cancel() {

			}
		});
	}

	public static DataStreamSource<Tuple2<GaussianGenerator,Boolean>> generateStream2(StreamExecutionEnvironment env, final Parameters initParams) {
		return env.addSource(new RichSourceFunction<Tuple2<GaussianGenerator,Boolean>>() {

			long count = 0;
			GaussianGenerator gaussD = new GaussianGenerator(initParams.getMeanInit(), initParams.getSigmaInit());

			@Override
			public void run(Collector<Tuple2<GaussianGenerator,Boolean>> collector) throws Exception {

				while (count < MAX_COUNT) {
					count++;
					gaussD.updateMean(count, initParams.getmStep(), initParams.getInterval());
					gaussD.updateSigma(count, initParams.getsStep(), initParams.getInterval());
					//double newItem = Gaussian.generate(gaussD.getMean(), gaussD.getSigma());

					collector.collect(new Tuple2<GaussianGenerator,Boolean>(gaussD, true));
				}
			}

			@Override
			public void cancel() {

			}
		});
	}

}
