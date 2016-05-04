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
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.windowing.Cutty;
import org.apache.flink.streaming.api.operators.windowing.GeneralMultiDiscretizer;
import org.apache.flink.streaming.api.operators.windowing.PairDiscretization;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("serial")
public class AggregationFramework {

	public enum AGGREGATION_STRATEGY {EAGER, LAZY}

	public enum DISCRETIZATION_TYPE {B2B, PAIRS}

	public static class WindowAggregation<Agg extends Serializable, In, Out> {

		private final MapFunction<Agg, Out> lift;
		private final ReduceFunction<Agg> combine;
		private final MapFunction<In, Agg> lower;
		private final Agg identityValue;


		public WindowAggregation(MapFunction<Agg, Out> lift,
								 ReduceFunction<Agg> combine, MapFunction<In, Agg> lower, Agg identityValue) {
			this.lift = lift;
			this.combine = combine;
			this.lower = lower;
			this.identityValue = identityValue;
		}

		@SuppressWarnings({"unchecked", "rawtypes"})
		public DataStream<Tuple2<Integer, Out>> applyOn(
				DataStream<In> input,
				Tuple3<List<DeterministicPolicyGroup<In>>, List<TriggerPolicy<In>>,
						List<EvictionPolicy<In>>> policies,
				AGGREGATION_STRATEGY aggType, DISCRETIZATION_TYPE discType) {

			TypeInformation<Agg> aggTypeInfo = TypeExtractor.getMapReturnTypes(lower, input.getType());
			TypeInformation<Tuple2<In, Agg>> lowerRetType = new TupleTypeInfo<>(
					input.getType(), aggTypeInfo);

			TypeInformation<Tuple2<Integer, Agg>> combinedType = new TupleTypeInfo<>(
					BasicTypeInfo.INT_TYPE_INFO, aggTypeInfo);
			TypeInformation<Tuple2<Integer, Out>> outputType = new TupleTypeInfo<>(
					BasicTypeInfo.INT_TYPE_INFO, TypeExtractor.getMapReturnTypes(lift, aggTypeInfo));


			DataStream<Tuple2<In, Agg>> lower = input.map(new Lower<>(this.lower)).startNewChain()
					.returns(lowerRetType);
			DataStream<Tuple2<Integer, Agg>> combinedWithID = null;

			switch (discType) {
				case B2B:
					if ((policies.f1 == null || policies.f1.isEmpty()) && (policies.f2 == null || policies.f2.isEmpty())) {
						combinedWithID = lower.transform("WindowAggregation", combinedType,
								new Cutty<>(
										policies.f0,
										combine,
										identityValue, 4, aggTypeInfo.createSerializer(null), aggType));
					} else {
						combinedWithID = lower.transform("WindowAggregation", combinedType,
								new GeneralMultiDiscretizer<>(
										policies.f1,
										policies.f2,
										combine,
										aggTypeInfo.createSerializer(null), identityValue, aggType));
					}
					break;
				case PAIRS:
					combinedWithID = lower.transform("WindowAggregation", combinedType,
							PairDiscretization.create(policies.f0, combine, identityValue, 4,
									aggTypeInfo.createSerializer(null), aggType));
			}

			return (DataStream<Tuple2<Integer, Out>>) combinedWithID.map(new Lift(this.lift)).returns(outputType);
		}
	}

	public static class Lower<IN, AGG> extends WrappingFunction<MapFunction<IN, AGG>>
			implements MapFunction<IN, Tuple2<IN, AGG>> {

		public Lower(MapFunction<IN, AGG> lower) {
			super(lower);
		}

		@Override
		public Tuple2<IN, AGG> map(IN value) throws Exception {
			return new Tuple2<>(value, wrappedFunction.map(value));
		}
	}

	public static class Lift<Agg, Out> extends WrappingFunction<MapFunction<Agg, Out>>
			implements MapFunction<Tuple2<Integer, Agg>, Tuple2<Integer, Out>> {

		public Lift(MapFunction<Agg, Out> lift) {
			super(lift);
		}

		@Override
		public Tuple2<Integer, Out> map(Tuple2<Integer, Agg> value)
				throws Exception {
			return new Tuple2<>(value.f0, wrappedFunction.map(value.f1));
		}
	}
}
