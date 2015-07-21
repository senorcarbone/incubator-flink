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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.windowing.DeterministicMultiDiscretizer;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

import java.util.List;

@SuppressWarnings("serial")
public class AggregationUtils {

	public static class WindowAggregation<A> {

		private final MapFunction<Double, A> lift;
		private final ReduceFunction<A> combine;
		private final MapFunction<A, Double> lower;
		private final A identityValue;

		WindowAggregation(MapFunction<Double, A> lift,
				ReduceFunction<A> combine, MapFunction<A, Double> lower, A identityValue) {
			this.lift = lift;
			this.combine = combine;
			this.lower = lower;
			this.identityValue = identityValue;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public DataStream<Tuple2<Integer,Double>> applyOn(
				DataStream<Tuple2<Double, Double>> input,
				Tuple3<List<DeterministicPolicyGroup<Tuple2<A, Double>>>, List<TriggerPolicy>, List<EvictionPolicy>> policies) {

			TypeInformation<Tuple2<A, Double>> liftReturnType = new TupleTypeInfo<Tuple2<A, Double>>(
					TypeExtractor.getMapReturnTypes(lift,
							BasicTypeInfo.DOUBLE_TYPE_INFO),
					BasicTypeInfo.DOUBLE_TYPE_INFO);

			DataStream<Tuple2<A, Double>> lifted = input.map(new Lift<A>(lift)).startNewChain()
					.returns(liftReturnType);

			TypeInformation<Tuple2<Integer, Tuple2<A, Double>>> combinedType = new TupleTypeInfo<Tuple2<Integer, Tuple2<A, Double>>>(
					BasicTypeInfo.INT_TYPE_INFO, liftReturnType);

			DataStream<Tuple2<Integer, Tuple2<A, Double>>> combinedWithID = lifted
					.transform("WindowAggregation", combinedType,
//							new MultiDiscretizer<Tuple2<A, Double>>(
//									policies.f0,
//									policies.f1,
//									policies.f2,
//									new Combine<A>(combine)));
					new DeterministicMultiDiscretizer<Tuple2<A, Double>>(
							policies.f0,
							new Combine<A>(combine),
							new Tuple2<A, Double>(identityValue, 0d),
							20, liftReturnType.createSerializer(null)));

			return combinedWithID.map(new Lower(lower));
		}
	}
	
	public static WindowAggregation<Tuple3<Integer, Double, Double>> StdAggregation = new WindowAggregation<Tuple3<Integer, Double, Double>>(
			new MapFunction<Double, Tuple3<Integer, Double, Double>>() {

				@Override
				public Tuple3<Integer, Double, Double> map(Double value)
						throws Exception {
					return new Tuple3<Integer, Double, Double>(1, value, value
							* value);
				}
			}, new ReduceFunction<Tuple3<Integer, Double, Double>>() {

				@Override
				public Tuple3<Integer, Double, Double> reduce(
						Tuple3<Integer, Double, Double> v1,
						Tuple3<Integer, Double, Double> v2) throws Exception {
					v1.f0 = v1.f0 + v2.f0;
					v1.f1 = v1.f1 + v2.f1;
					v1.f2 = v1.f2 + v2.f2;
					return v1;
				}

			}, new MapFunction<Tuple3<Integer, Double, Double>, Double>() {

				@Override
				public Double map(Tuple3<Integer, Double, Double> value)
						throws Exception {
					return Math
							.sqrt((value.f2 - (value.f1 * value.f1 / value.f0))
									/ value.f0);
				}

			}, new Tuple3<Integer, Double, Double>(0,0d,0d));
	
	public static WindowAggregation<Double> SumAggregation = new WindowAggregation<Double>(
			new IdMap(), new ReduceFunction<Double>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Double reduce(Double value1, Double value2)
						throws Exception {
					return value1 + value2;
				}
			}, new IdMap(), 0d);

	public static class IdMap implements MapFunction<Double, Double> {

		@Override
		public Double map(Double value) throws Exception {
			return value;
		}

	}

	public static class Combine<A> implements ReduceFunction<Tuple2<A, Double>> {

		ReduceFunction<A> combine;

		Combine(ReduceFunction<A> combine) {
			this.combine = combine;
		}

		@Override
		public Tuple2<A, Double> reduce(Tuple2<A, Double> value1,
				Tuple2<A, Double> value2) throws Exception {

			value1.f0 = combine.reduce(value1.f0, value2.f0);
			return value1;

		}
	}
	
	public static class Lower<A> implements MapFunction<Tuple2<Integer,Tuple2<A, Double>>,Tuple2<Integer,Double>>{

		MapFunction<A, Double> lower;
		
		private Lower(MapFunction<A, Double> lower){
			this.lower = lower;
		}
		
		@Override
		public Tuple2<Integer, Double> map(
				Tuple2<Integer, Tuple2<A, Double>> value) throws Exception {
			return new Tuple2<Integer, Double>(value.f0, lower.map(value.f1.f0));
		}
		
	}

	public static class Lift<A> implements
			MapFunction<Tuple2<Double, Double>, Tuple2<A, Double>> {

		private MapFunction<Double, A> lift;

		Lift(MapFunction<Double, A> lift) {
			this.lift = lift;
		}

		@Override
		public Tuple2<A, Double> map(Tuple2<Double, Double> value)
				throws Exception {
			return new Tuple2<A, Double>(lift.map(value.f0), value.f1);
		}

	}

}
