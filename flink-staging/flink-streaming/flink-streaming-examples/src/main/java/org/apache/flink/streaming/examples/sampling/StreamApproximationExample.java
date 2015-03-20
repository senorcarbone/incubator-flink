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

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.sink.RichSinkFunction;
import org.apache.flink.streaming.api.function.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.*;

import java.util.Random;

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
		//evaluationThroughIterator(env, initParams);
		evaluateSampling(env, initParams);
		env.execute();
	} //end of StreamApproximationExample cTor


	public static void main(String args[]) throws Exception {
		new StreamApproximationExample();
	}


	public static void evaluateSampling(StreamExecutionEnvironment env, final Parameters params) {
		//create source
		DataStreamSource<Distribution> source = createSource(env, params);
		source.map(new MapFunction<Distribution, Double>() {
			@Override
			public Double map(Distribution value) throws Exception {
				return value.nextGaussian();
			}
		}).map(new RichMapFunction<Double, Reservoir<Double>>() {
			Reservoir<Double> r = new Reservoir<Double>(params.getSampleSize());
			int count = 0;

			@Override
			public Reservoir<Double> map(Double value) throws Exception {
				count++;

				/*RuntimeContext context = getRuntimeContext();
				if (context.getIndexOfThisSubtask() == 0) {}*/

				if (Coin.flip(count / params.getSampleSize())) {
					r.insertElement(value);
				}
				return r;
			}
		})
				.connect(source)
				.flatMap(new CoFlatMapFunction<Reservoir<Double>, Distribution, Double>() {
					Distribution currentDist = new Distribution();

					@Override
					public void flatMap1(Reservoir<Double> value, Collector<Double> out) throws Exception {
						Distribution sampledDist = new Distribution(value);
						//System.out.println(currentDist.toString() + " " + sampledDist.toString());
						out.collect(Evaluator.evaluate(currentDist, sampledDist));
					}

					@Override
					public void flatMap2(Distribution value, Collector<Double> out) throws Exception {
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
	 * does not work due to broken iterations.
	 *
	 * @param env
	 * @param initParams
	 */
	public static void evaluationThroughIterator(StreamExecutionEnvironment env, final Parameters initParams) {

		//create source
		DataStreamSource<Distribution> source = createSource(env, initParams);

		//create iteration
		IterativeDataStream<Tuple2<Distribution, Boolean>> iteration = source.map(new MapFunction<Distribution, Tuple2<Distribution, Boolean>>() {
			@Override
			public Tuple2<Distribution, Boolean> map(Distribution value) throws Exception {
				return new Tuple2<Distribution, Boolean>(value, true);
			}
		}).iterate();

		//define iteration head
		DataStream<Tuple2<Double, Boolean>> head = iteration.map(new MapFunction<Tuple2<Distribution, Boolean>, Tuple2<Double, Boolean>>() {

			Distribution currentDist = new Distribution(initParams.getMeanInit(), initParams.getSigmaInit());

			@Override
			public Tuple2<Double, Boolean> map(Tuple2<Distribution, Boolean> value) throws Exception {
				if (value.f1) {
					// must save latest distribution value

					currentDist = value.f0;

					// must be forwarded to sampler (flag should be true to pass the isFeedback filter)
					return new Tuple2<Double, Boolean>(value.f0.nextGaussian(), true);
				} else {

					// must be compared with latest distribution value and evaluated
					double distance = Evaluator.evaluate(currentDist, value.f0);
					return new Tuple2<Double, Boolean>(distance, false);
				}

			}
		});

		//define iteration tail
		DataStream<Tuple2<Distribution, Boolean>> tail = head.filter(new FilterFunction<Tuple2<Double, Boolean>>() {
			@Override
			//allow tuples generated from source to pass to the sampling phase
			//is feedback
			public boolean filter(Tuple2<Double, Boolean> value) throws Exception {
				return value.f1;
			}
			//SAMPLING ALGORITHM
			//RESERVOIR SAMPLING
		}).map(new MapFunction<Tuple2<Double, Boolean>, Tuple2<Distribution, Boolean>>() {
			Reservoir<Double> r = new Reservoir<Double>(initParams.getSampleSize());
			int count = 0;

			@Override
			public Tuple2<Distribution, Boolean> map(Tuple2<Double, Boolean> value) throws Exception {
				count++;
				if (Coin.flip(count / initParams.getSampleSize())) {
					r.insertElement(value.f0);
				}

				return new Tuple2<Distribution, Boolean>(new Distribution(r), false);
			}
		});

		//close iteration with tail
		iteration.closeWith(tail);

		//filter out results from evaluator and sink
		head.filter(new FilterFunction<Tuple2<Double, Boolean>>() {
			@Override
			public boolean filter(Tuple2<Double, Boolean> value) throws Exception {
				return !value.f1;
			}
		}).addSink(new RichSinkFunction<Tuple2<Double, Boolean>>() {
			@Override
			public void invoke(Tuple2<Double, Boolean> value) throws Exception {
				System.out.println("Distance: " + value.f0);
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
	public static DataStreamSource<Distribution> createSource(StreamExecutionEnvironment env, final Parameters params) {
		return env.addSource(new RichSourceFunction<Distribution>() {

			long count = 0;
			Distribution gaussD = new Distribution(params.getMeanInit(), params.getSigmaInit());

			@Override
			public void run(Collector<Distribution> collector) throws Exception {

				while (count < MAX_COUNT) {
					count++;
					gaussD.updateMean(count, params.getmStep(), params.getInterval());
					gaussD.updateSigma(count, params.getsStep(), params.getInterval());
					//double newItem = Gaussian.nextGaussian(gaussD.getMean(), gaussD.getSigma());

					collector.collect(gaussD);
				}
			}

			@Override
			public void cancel() {

			}
		});
	}

	public static DataStreamSource<Tuple2<Distribution, Boolean>> generateStream2(StreamExecutionEnvironment env, final Parameters initParams) {
		return env.addSource(new RichSourceFunction<Tuple2<Distribution, Boolean>>() {

			long count = 0;
			Distribution gaussD = new Distribution(initParams.getMeanInit(), initParams.getSigmaInit());

			@Override
			public void run(Collector<Tuple2<Distribution, Boolean>> collector) throws Exception {

				while (count < MAX_COUNT) {
					count++;
					gaussD.updateMean(count, initParams.getmStep(), initParams.getInterval());
					gaussD.updateSigma(count, initParams.getsStep(), initParams.getInterval());
					//double newItem = Gaussian.nextGaussian(gaussD.getMean(), gaussD.getSigma());

					collector.collect(new Tuple2<Distribution, Boolean>(gaussD, true));
				}
			}

			@Override
			public void cancel() {

			}
		});
	}


	private static final class Coin {
		public static boolean flip(int sides) {
			return (Math.random() * sides < 1);
		}
	}

	private static final class Gaussian {
		public static double nextGaussian(double mean, double stDev) {
			return (new Random().nextGaussian() * stDev + mean);
		}
	}

}
