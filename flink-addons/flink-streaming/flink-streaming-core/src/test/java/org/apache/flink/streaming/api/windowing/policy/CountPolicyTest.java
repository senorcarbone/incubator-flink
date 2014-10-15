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

package org.apache.flink.streaming.api.windowing.policy;

import com.google.common.collect.Lists;

import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;


public class CountPolicyTest {


	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testDelta() {

		TriggerPolicy triggerPolicy = Count.of(2).toTrigger();
		EvictionPolicy evictPolicy = Count.of(5).toEvict();

		ArrayList tuples = Lists.newArrayList(1,2,3,4,5,6,7);

		assertFalse(triggerPolicy.notifyTrigger(tuples.get(0)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(0), false, 0));

		assertFalse(triggerPolicy.notifyTrigger(tuples.get(1)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(1), false, 1));

		assertTrue(triggerPolicy.notifyTrigger(tuples.get(2)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(2), true, 2));

		assertFalse(triggerPolicy.notifyTrigger(tuples.get(3)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(3), true, 3));

		assertTrue(triggerPolicy.notifyTrigger(tuples.get(4)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(4), true, 4));

		assertFalse(triggerPolicy.notifyTrigger(tuples.get(5)));
		assertEquals(1, evictPolicy.notifyEviction(tuples.get(5), true, 5));

		assertTrue(triggerPolicy.notifyTrigger(tuples.get(6)));
		assertEquals(1, evictPolicy.notifyEviction(tuples.get(6), true, 5));
	}

}
