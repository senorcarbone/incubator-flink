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

package org.apache.flink.streaming.api.operators.windowing;

import junit.framework.TestCase;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.paper.AggregationFramework;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class AllMultiDiscretizersTest extends TestCase {

	/*********************************************
	 * Data                                      *
	 *********************************************/

	List<Tuple2<Integer, Integer>> inputs1 = new ArrayList<>();
	List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> inputs2 = new ArrayList<>();

	{
		inputs1.add(new Tuple2<>(1, 1));
		inputs1.add(new Tuple2<>(2, 2));
		inputs1.add(new Tuple2<>(2, 2));
		inputs1.add(new Tuple2<>(10, 10));
		inputs1.add(new Tuple2<>(11, 11));
		inputs1.add(new Tuple2<>(14, 14));
		inputs1.add(new Tuple2<>(16, 16));
		inputs1.add(new Tuple2<>(21, 21));

		inputs2.add(new Tuple2<>(new Tuple2<>(1, 0), new Tuple2<>(1, 0)));
		inputs2.add(new Tuple2<>(new Tuple2<>(2, 1), new Tuple2<>(2, 1)));
		inputs2.add(new Tuple2<>(new Tuple2<>(2, 2),new Tuple2<>(2, 2)));
		inputs2.add(new Tuple2<>(new Tuple2<>(10, 3),new Tuple2<>(10, 3)));
		inputs2.add(new Tuple2<>(new Tuple2<>(11, 4),new Tuple2<>(11, 4)));
		inputs2.add(new Tuple2<>(new Tuple2<>(14, 5),new Tuple2<>(14, 5)));
		inputs2.add(new Tuple2<>(new Tuple2<>(16, 6),new Tuple2<>(16, 6)));
		inputs2.add(new Tuple2<>(new Tuple2<>(21, 7),new Tuple2<>(21, 7)));
	}

	/*********************************************
	 * Tests                                     *
	 *********************************************/

	@Test
	public void testMultiDiscretizerDeterministic() {

		//prepare expected result
		LinkedList<Tuple2<Integer, Integer>> expected = new LinkedList<>();
		expected.add(new Tuple2<>(0, 5));  //0..4
		expected.add(new Tuple2<>(0, 5));  //0..9
		expected.add(new Tuple2<>(0, 35)); //5..14
		expected.add(new Tuple2<>(0, 51)); //10..19

		//prepare policies
		@SuppressWarnings("unchecked")
		TimestampWrapper<Integer> timestampWrapper = new TimestampWrapper<>(value -> value, 0);
		DeterministicTriggerPolicy<Integer> triggerPolicy = new DeterministicTimeTriggerPolicy<>(5, timestampWrapper);
		DeterministicEvictionPolicy<Integer> evictionPolicy = new DeterministicTimeEvictionPolicy<>(10, timestampWrapper);
		DeterministicPolicyGroup<Integer> policyGroup = new DeterministicPolicyGroup<>(triggerPolicy, evictionPolicy, new IntegerToDouble());

		LinkedList<DeterministicPolicyGroup<Integer>> policyGroups = new LinkedList<>();
		policyGroups.add(policyGroup);

		//Create operator instance
		Cutty<Integer,Integer> deterministicMD = new Cutty<>
				(policyGroups, new Sum(), 0, 4, IntSerializer.INSTANCE, AggregationFramework.AGGREGATION_STRATEGY.LAZY);

		//Run the test
		List<Tuple2<Integer, Integer>> result = MockContext.createAndExecute(deterministicMD, this.inputs1);

		//check correctness
		assertEquals(expected, result);
	}

	@Test
	public void testMultiDiscretizerMultipleDeterministic() {

		//prepare expected result
		LinkedList<Tuple2<Integer, Tuple2<Integer, Integer>>> expected = new LinkedList<>();
		expected.add(new Tuple2<>(1,
				new Tuple2<>(3, 1)));    //Q1 seq 0,1
		expected.add(new Tuple2<>(0,
				new Tuple2<>(5, 3)));   //Q0 0..4
		expected.add(new Tuple2<>(0,
				new Tuple2<>(5, 3)));   //Q0 0..9
		expected.add(new Tuple2<>(1,
				new Tuple2<>(15, 6)));   //Q1 seq 0,1,2,3
		expected.add(new Tuple2<>(0,
				new Tuple2<>(35, 12))); //Q0 5..14
		expected.add(new Tuple2<>(1,
				new Tuple2<>(39, 15)));  //Q1 seq 1,2,3,4,5
		expected.add(new Tuple2<>(0,
				new Tuple2<>(51, 18))); //Q0 10..19

		//prepare policies
		@SuppressWarnings("unchecked")
		TimestampWrapper<Tuple2<Integer, Integer>> timestampWrapper = new TimestampWrapper<>(value -> value.f0, 0);
		
		DeterministicTriggerPolicy<Tuple2<Integer, Integer>> triggerPolicy =
				new DeterministicTimeTriggerPolicy<>(5, timestampWrapper);
		DeterministicEvictionPolicy<Tuple2<Integer, Integer>> evictionPolicy =
				new DeterministicTimeEvictionPolicy<>(10, timestampWrapper);
		DeterministicPolicyGroup<Tuple2<Integer, Integer>> policyGroup =
				new DeterministicPolicyGroup<>(triggerPolicy, evictionPolicy, new Tuple2ToDouble(0));

		DeterministicTriggerPolicy<Tuple2<Integer, Integer>> triggerPolicy2 =
				new DeterministicCountTriggerPolicy<>(2);
		DeterministicEvictionPolicy<Tuple2<Integer, Integer>> evictionPolicy2 =
				new DeterministicCountEvictionPolicy<>(5);
		DeterministicPolicyGroup<Tuple2<Integer, Integer>> policyGroup2 =
				new DeterministicPolicyGroup<>(triggerPolicy2, evictionPolicy2, new Tuple2ToDouble(1));

		LinkedList<DeterministicPolicyGroup<Tuple2<Integer, Integer>>> policyGroups =
				new LinkedList<>();
		policyGroups.add(policyGroup);
		policyGroups.add(policyGroup2);

		Cutty<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> deterministicMD =
				new Cutty<>(policyGroups, new TupleSum(),
						new Tuple2<>(0, 0), 8, new TupleTypeInfo<Tuple2<Integer, Integer>>
						(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO).createSerializer(null),
						AggregationFramework.AGGREGATION_STRATEGY.EAGER);

		//Run the test
		List<Tuple2<Integer, Tuple2<Integer, Integer>>> result = MockContext.createAndExecute(deterministicMD, inputs2);

		//check correctness
		assertEquals(expected, result);
	}


	@Test
	public void testMultiDiscretizerNotDeterministic() {

		//prepare expected result
		LinkedList<Tuple2<Integer, Integer>> expected = new LinkedList<>();
		expected.add(new Tuple2<>(0, 5));  //0..4
		expected.add(new Tuple2<>(0, 5));  //0..9
		expected.add(new Tuple2<>(0, 35)); //5..14
		expected.add(new Tuple2<>(0, 51)); //10..19

		//prepare policies
		@SuppressWarnings("unchecked")
		TimestampWrapper<Integer> timestampWrapper = new TimestampWrapper<>(value -> value,0);
		TriggerPolicy<Integer> triggerPolicy = new TimeTriggerPolicy<>(5, timestampWrapper);
		EvictionPolicy<Integer> evictionPolicy = new TimeEvictionPolicy<>(10, timestampWrapper);

		List<TriggerPolicy<Integer>> triggerPolicies = new LinkedList<>();
		triggerPolicies.add(triggerPolicy);
		List<EvictionPolicy<Integer>> evictionPolicies = new LinkedList<>();
		evictionPolicies.add(evictionPolicy);

		//Create operator instance
		GeneralMultiDiscretizer<Integer, Integer> nonDeterministicMD =
				new GeneralMultiDiscretizer<>(triggerPolicies, evictionPolicies, new Sum(), IntSerializer.INSTANCE, 0,
						AggregationFramework.AGGREGATION_STRATEGY.LAZY);

		//Run the test
		List<Tuple2<Integer, Integer>> result = MockContext.createAndExecute(nonDeterministicMD, inputs1);

		//check correctness
		assertEquals(expected, result);
	}

	@Test
	public void testMultiDiscretizerMultipleNotDeterministic() {

		//prepare expected result
		LinkedList<Tuple2<Integer, Tuple2<Integer, Integer>>> expected = new LinkedList<>();
		expected.add(new Tuple2<>(1, new Tuple2<>(3, 1)));    //Q1 seq 0,1
		expected.add(new Tuple2<>(0, new Tuple2<>(5, 3)));   //Q0 0..4
		expected.add(new Tuple2<>(0, new Tuple2<>(5, 3)));   //Q0 0..9
		expected.add(new Tuple2<>(1, new Tuple2<>(15, 6)));   //Q1 seq 0,1,2,3
		expected.add(new Tuple2<>(0, new Tuple2<>(35, 12))); //Q0 5..14
		expected.add(new Tuple2<>(1, new Tuple2<>(39, 15)));  //Q1 seq 1,2,3,4,5
		expected.add(new Tuple2<>(0, new Tuple2<>(51, 18))); //Q0 10..19

		//prepare policies
		@SuppressWarnings("unchecked")
		TimestampWrapper<Tuple2<Integer, Integer>> timestampWrapper =
				new TimestampWrapper<>(value -> value.f0, 0);
		TriggerPolicy<Tuple2<Integer, Integer>> triggerPolicy =
				new TimeTriggerPolicy<>(5, timestampWrapper);
		EvictionPolicy<Tuple2<Integer, Integer>> evictionPolicy =
				new TimeEvictionPolicy<>(10, timestampWrapper);

		TriggerPolicy<Tuple2<Integer, Integer>> triggerPolicy2 =
				new CountTriggerPolicy<>(2);
		EvictionPolicy<Tuple2<Integer, Integer>> evictionPolicy2 =
				new CountEvictionPolicy<>(5);

		LinkedList<TriggerPolicy<Tuple2<Integer, Integer>>> triggerPolicies =
				new LinkedList<>();
		triggerPolicies.add(triggerPolicy);
		triggerPolicies.add(triggerPolicy2);
		LinkedList<EvictionPolicy<Tuple2<Integer, Integer>>> evictionPolicies =
				new LinkedList<>();
		evictionPolicies.add(evictionPolicy);
		evictionPolicies.add(evictionPolicy2);

		//Create operator instance
		GeneralMultiDiscretizer<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> nonDeterministicMD =
				new GeneralMultiDiscretizer<>(triggerPolicies, evictionPolicies, new TupleSum(),
						new TupleTypeInfo<Tuple2<Integer, Integer>> (BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO).createSerializer(null),
						new Tuple2<>(0, 0), AggregationFramework.AGGREGATION_STRATEGY.EAGER);

		//Run the test
		List<Tuple2<Integer, Tuple2<Integer, Integer>>> result = MockContext.createAndExecute(nonDeterministicMD, inputs2);

		//check correctness
		assertEquals(expected, result);
	}

	/*********************************************
	 * Utilities                                 *
	 *********************************************/

	private class Sum implements ReduceFunction<Integer> {

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}

	}

	private class TupleSum implements ReduceFunction<Tuple2<Integer, Integer>> {

		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
			return new Tuple2<>(value1.f0 + value2.f0, value1.f1 + value2.f1);
		}
	}

	private class IntegerToDouble implements Extractor<Integer, Double> {

		@Override
		public Double extract(Integer in) {
			return in.doubleValue();
		}
	}

	private class Tuple2ToDouble implements Extractor<Tuple2<Integer, Integer>, Double> {

		int field;

		public Tuple2ToDouble(int field) {
			this.field = field;
		}

		@Override
		public Double extract(Tuple2<Integer, Integer> in) {
			return ((Integer) in.getField(this.field)).doubleValue();
		}
	}
}