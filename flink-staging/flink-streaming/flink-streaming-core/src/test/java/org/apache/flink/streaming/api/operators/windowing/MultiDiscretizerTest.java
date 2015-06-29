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

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.*;
import org.apache.flink.streaming.util.MockContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MultiDiscretizerTest {

    @Test
    public void testMultiDiscretizerDeterministic(){

        //prepare input data
        List<Integer> inputs = new ArrayList<Integer>();
        inputs.add(1);
        inputs.add(2);
        inputs.add(2);
        inputs.add(10);
        inputs.add(11);
        inputs.add(14);
        inputs.add(16);
        inputs.add(21);

        //prepare expected result
        LinkedList<Tuple2<Integer,Integer>> expected = new LinkedList<Tuple2<Integer,Integer>>();
        expected.add(new Tuple2<Integer, Integer>(0, 5));  //0..4
        expected.add(new Tuple2<Integer, Integer>(0, 5));  //0..9
        expected.add(new Tuple2<Integer, Integer>(0, 35)); //5..14
        expected.add(new Tuple2<Integer, Integer>(0, 51)); //10..19

        //prepare policies
        @SuppressWarnings("unchecked")
        TimestampWrapper<Integer> timestampWrapper=new TimestampWrapper<Integer>(new Timestamp() {
            @Override
            public long getTimestamp(Object value) {
                return ((Integer)value);
            }
        },0);
        DeterministicTriggerPolicy<Integer> triggerPolicy=new DeterministicTimeTriggerPolicy<Integer>(5,timestampWrapper);
        DeterministicEvictionPolicy<Integer> evictionPolicy=new DeterministicTimeEvictionPolicy<Integer>(10,timestampWrapper);
        DeterministicPolicyGroup<Integer> policyGroup=new DeterministicPolicyGroup<Integer>(triggerPolicy, evictionPolicy, new IntegerToDuble());

        LinkedList<DeterministicPolicyGroup<Integer>> policyGroups=new LinkedList<DeterministicPolicyGroup<Integer>>();
        policyGroups.add(policyGroup);
        LinkedList<TriggerPolicy<Integer>> triggerPolicies=new LinkedList<TriggerPolicy<Integer>>();
        LinkedList<EvictionPolicy<Integer>> evictionPolicies=new LinkedList<EvictionPolicy<Integer>>();

        //Create operator instance
        MultiDiscretizer<Integer> multiDiscretizer=new MultiDiscretizer<Integer>(policyGroups,triggerPolicies,evictionPolicies,new Sum());

        //Run the test
        List<Tuple2<Integer, Integer>> result = MockContext.createAndExecute(multiDiscretizer, inputs);

        //check correctness
        assertEquals(expected,result);
    }

    //TODO add more test cases here to cover different setups

    /*********************************************
    Utilities
     *********************************************/

    private class Sum implements ReduceFunction<Integer>{

        @Override
        public Integer reduce(Integer value1, Integer value2) throws Exception {
            return value1+value2;
        }

    }

    private class IntegerToDuble implements Extractor<Integer,Double>{

        @Override
        public Double extract(Integer in) {
            return in.doubleValue();
        }
    }
}
