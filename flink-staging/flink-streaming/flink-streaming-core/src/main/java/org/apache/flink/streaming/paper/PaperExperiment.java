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

package org.apache.flink.streaming.paper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.streaming.paper.AggregationUtils.SumAggregation;

@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
public class PaperExperiment {


	private static Random rnd = new Random();

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(1);

		DataStream<Tuple2<Double, Double>> source = env
				.addSource(new DataGenerator(1000));

		SumAggregation.applyOn(source, getTestPolicies(3))
				.map(new Prefix("SUM")).print();
		// StdAggregation.applyOn(source, getTestPolicies(3))
		// .map(new Prefix("STD")).print();

		env.execute();
	}

	public static Tuple3<List<DeterministicPolicyGroup<Tuple2<Double, Double>>>, List<TriggerPolicy>, List<EvictionPolicy>> getTestPolicies(
			int nrOfDeterministicPolicies) {

		LinkedList<DeterministicPolicyGroup<Tuple2<Double,Double>>> deterministicGroups = new LinkedList();
		for (int i = 0; i < nrOfDeterministicPolicies; i++) {
			deterministicGroups.add(generateDeterministicPolicyGroup());
		}

		return new Tuple3<List<DeterministicPolicyGroup<Tuple2<Double,Double>>>, List<TriggerPolicy>, List<EvictionPolicy>>(
				deterministicGroups, emptyList, emptyList);
	}

	static LinkedList emptyList = new LinkedList();

	static Extractor<Tuple2<Double, Double>, Double> extractor = new Extractor<Tuple2<Double, Double>, Double>() {
		@Override
		public Double extract(Tuple2<Double, Double> in) {
			return in.f1;
		}
	};

	private static DeterministicPolicyGroup generateDeterministicPolicyGroup() {
		DeterministicPolicyGroup<Tuple2<Double, Double>> group;

		boolean isTime = rnd.nextBoolean();
		if (isTime) {

			TimestampWrapper tsw = new TimestampWrapper(new Timestamp<Tuple2<Double,Double>>() {

				@Override
				public long getTimestamp(Tuple2<Double,Double> value) {
					return value.f1.longValue();
				}
			}, 0);
			group = new DeterministicPolicyGroup<Tuple2<Double, Double>>(
					new DeterministicTimeTriggerPolicy(rnd.nextInt(5) + 1, tsw),
					new DeterministicTimeEvictionPolicy(rnd.nextInt(5) + 1, tsw),
					extractor);
		} else {
			group = new DeterministicPolicyGroup<Tuple2<Double, Double>>(
					new DeterministicCountTriggerPolicy(rnd.nextInt(5) + 1),
					new DeterministicCountEvictionPolicy(rnd.nextInt(5) + 1),
					extractor);
		}

		return group;
	}

	public static class Prefix implements
			MapFunction<Tuple2<Integer, Double>, String> {

		private String s;

		public Prefix(String s) {
			this.s = s;
		}

		@Override
		public String map(Tuple2<Integer, Double> value) throws Exception {
			return s + " - " + value;
		}

	}

	public static class DataGenerator implements
			SourceFunction<Tuple2<Double, Double>> {

		private static final long serialVersionUID = 1L;

		volatile boolean isRunning = false;
		Random rnd;
		int sleepMillis;
		double c = 1;

		public DataGenerator(int sleepMillis) {
			this.sleepMillis = sleepMillis;
		}

		@Override
		public void run(SourceContext<Tuple2<Double, Double>> ctx)
				throws Exception {
			isRunning = true;
			rnd = new Random();
			while (isRunning) {
				ctx.collect(new Tuple2<Double, Double>((double) rnd
						.nextInt(1000), c++));
				Thread.sleep(sleepMillis);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

	}

}
