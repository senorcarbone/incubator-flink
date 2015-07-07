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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.streaming.sampling.airlines.SimpleComparator;
import org.apache.flink.streaming.sampling.helpers.Configuration;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.samplers.StreamSampler;
import org.apache.flink.streaming.sampling.samplers.UniformSampler;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by marthavk on 2015-05-21.
 * <p/>
 * Tuple2 has the fields: f0->Integer[] and f1->String[]
 * <p/>
 * Integer[] is an array of 11 values containing in the following order:
 * [year, month, day of month, day of week, CRS depart time, CRS arrival time
 * flight number, actual elapsed time, distance, diverted, delay]
 * <p/>
 * String[] is an array of 3 values containing in the following order:
 * [unique carrier, origin, destination]
 */

public class AirlinesExample implements Serializable {

	/*properties*/
	static String path = Configuration.path;

	/*query properties*/
	static final String source_file = path + "january_dataset";
	final static int size_of_stream = 10000;

	public static void main(String[] args) throws Exception {


		/*set sample and window sizes*/
		final int sample_size = Configuration.SAMPLE_SIZE_1000;
		final int sampling_rate = 100;
		long time_window_size = Configuration.timeWindowSize;
		final int count_window_size = Configuration.countWindowSize;

		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		/*set source*/
		DataStreamSource<String> source = env.readTextFile(source_file);

		/*
		 * Tuple8 fields:
		 * f0 day of january (Integer)
		 * f1 day of week (Integer)
		 * f2 crs depart time (Integer)
		 * f3 unique carrier (String)
		 * f4 origin (String)
		 * f5 destination (String)
		 * f6 delay (Integer)
		 * f7 1
		 */
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>, ?> dataStream =

				source.map(new RichMapFunction<String, Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>>() {

					@Override
					public Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer> map(String record) throws Exception {
						Tuple8 out = new Tuple8();
						String[] values = record.split(",");
/*						int[] integerFields = new int[]{0, 1, 2};
						int[] stringFields = new int[]{3, 4, 5};
						int[] integerFields2 = new int[]{6, 7};*/


						int[] integerFields = new int[]{1,2,3};
						int[] stringFields = new int[]{4, 5, 6};
						int[] integerFields2 = new int[]{7, 8};

						int counter = 0;
						for (int i : integerFields) {

							int curr = Integer.parseInt(values[i]);
							out.setField(curr, counter);
							counter++;
						}

						for (int i : stringFields) {
							out.setField(values[i], counter);
							counter++;
						}

						for (int i : integerFields2) {
							int curr = Integer.parseInt(values[i]);
							out.setField(curr, counter);
							counter++;
						}

						//StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
						//System.out.println(context.getIndexOfThisSubtask() + ">  " + out.toString());
						return out;

					}
				}).setParallelism(1);

		//SAMPLE
		UniformSampler uniformSampler = new UniformSampler(sample_size, sampling_rate);
		SingleOutputStreamOperator sample = dataStream.transform("sampleRS", dataStream.getType(), new StreamSampler<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>>(uniformSampler));
		sample.print();

		//AGGREGATES
	//	calculateAggregates(sample);

		//HEAVY HITTERS
	//	heavyHitters(sample);

		//RANGE QUERIES for the first and last week of january
	//	rangeQueries(sample);


		/*get js for execution plan*/
		System.err.println(env.getExecutionPlan());

		/*execute program*/
		env.execute();

	}

	/** METHODS **/

	public static void calculateAggregates(DataStream<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>> ds) {
		ds.groupBy(3).sum(7).flatMap(new FlatMapFunction<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>, HashMap<String, Integer>>() {
			int counter = 0;
			HashMap<String, Integer> carriers = new HashMap<String, Integer>();

			@Override
			public void flatMap(Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer> value, Collector<HashMap<String, Integer>> out) throws Exception {
				counter++;
				carriers.put(value.f3, value.f7);
				if (counter == size_of_stream) {
					out.collect(carriers);
				}
			}
		}).setParallelism(1).map(new MapFunction<HashMap<String, Integer>, HashMap<String, Integer>>() {
			@Override
			public HashMap<String, Integer> map(HashMap<String, Integer> initMap) throws Exception {
				HashMap<String, Integer> subMap = new HashMap<String, Integer>();
				String[] aggregates = new String[]{"UA", "FL", "DL", "AS", "US", "AA", "MQ", "EV", "HA", "WN"};
				List aggregatesList = Arrays.asList(aggregates);
				for (String key : initMap.keySet()) {
					if (aggregatesList.contains(key)) {
						subMap.put(key, initMap.get(key));
					}
				}
				return subMap;
			}
		})
				.addSink(new RichSinkFunction<HashMap<String, Integer>>() {
					@Override
					public void invoke(HashMap<String, Integer> value) throws Exception {
						System.err.println(source_file);
						System.err.println("1) Aggregates: ");
						System.err.println(value.toString());
					}
				});

	}

	public static void heavyHitters(DataStream<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>> ds) {
		ds.groupBy(5).sum(7).flatMap(new FlatMapFunction<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>, HashMap<String, Integer>>() {
			int counter = 0;
			HashMap<String, Integer> destinations = new HashMap<String, Integer>();

			@Override
			public void flatMap(Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer> value, Collector<HashMap<String, Integer>> out) throws Exception {
				counter++;
				destinations.put(value.f5, value.f7);
				if (counter == size_of_stream) {
					out.collect(destinations);
				}

			}
		}).setParallelism(1).map(new MapFunction<HashMap<String, Integer>, TreeMap<String, Integer>>() {
			@Override
			public TreeMap<String, Integer> map(HashMap<String, Integer> destinations) throws Exception {
				SimpleComparator acomp = new SimpleComparator(destinations);
				TreeMap<String, Integer> sorted = new TreeMap<String, Integer>(acomp);
				sorted.putAll(destinations);
				return sorted;
			}
		}).addSink(new RichSinkFunction<TreeMap<String, Integer>>() {
			@Override
			public void invoke(TreeMap<String, Integer> value) throws Exception {
				System.err.println("2) Heavy Hitters: ");
				System.err.println(value.toString());

			}
		});
	}

	public static void rangeQueries(DataStream<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>> ds) {
		/*
		* q1 : all flights for the first/last week of January
		* q2 : all flights by HA carrier
		* q3 : all flights before 10:00
		* q4 : all flights to JFK airport
		* q5 : all flights delayed more than 20min
		* q6 : all flights on a weekend
		 */

		ds.flatMap(new FlatMapFunction<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
			int q1 = 0;
			int q2 = 0;
			int q3 = 0;
			int q4 = 0;
			int q5 = 0;
			int q6 = 0;
			int counter = 0;

			@Override
			public void flatMap(Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer> value, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
				if (value.f0 < 8) {
					q1++;
					if (value.f3.equals("HA")) {
						q2++;
					}
					if (value.f2 < 1000) {
						q3++;
					}
					if (value.f5.equals("JFK")) {
						q4++;
					}
					if (value.f6 > 20) {
						q5++;
					}
					if (value.f1 == 6 || value.f1 == 7) {
						q6++;
					}
				}

				counter++;

				if (counter == size_of_stream) {
					out.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(q1, q2, q3, q4, q5, q6));
				}
			}
		}).setParallelism(1).addSink(new RichSinkFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public void invoke(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> value) throws Exception {
				System.err.println("3) Range Queries for the FIRST week of January:");
				System.err.println(value.toString());
			}
		});

		ds.flatMap(new FlatMapFunction<Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
			int q1 = 0;
			int q2 = 0;
			int q3 = 0;
			int q4 = 0;
			int q5 = 0;
			int q6 = 0;
			int counter = 0;

			@Override
			public void flatMap(Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer> value, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
				if (value.f0 > 24) {
					q1++;
					if (value.f3.equals("HA")) {
						q2++;
					}
					if (value.f2 < 1000) {
						q3++;
					}
					if (value.f5.equals("JFK")) {
						q4++;
					}
					if (value.f6 > 20) {
						q5++;
					}
					if (value.f1 == 6 || value.f1 == 7) {
						q6++;
					}
				}

				counter++;

				if (counter == size_of_stream) {
					out.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(q1, q2, q3, q4, q5, q6));
				}
			}
		}).setParallelism(1).addSink(new RichSinkFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public void invoke(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> value) throws Exception {
				System.err.println("4) Range Queries for the LAST week of January:");
				System.err.println(value.toString());
			}
		});
	}

}
