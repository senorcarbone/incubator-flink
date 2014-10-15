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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.flink.streaming.api.windowing.helper.Count;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CountTriggerPolicyTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCountTriggerPolicy() {

		ArrayList tuples = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		int counter = 0;

		for (int i = 0; i < 10; i++) {
			TriggerPolicy triggerPolicy = Count.of(i).toTrigger();

			// Test first i steps (should not trigger)
			for (int j = 0; j < i; j++) {
				counter++;
				assertFalse("Triggerpolicy with count of " + i + " triggered at add nr. " + counter
						+ ". It should not trigger for the first " + i + " adds.",
						triggerPolicy.notifyTrigger(tuples.get(j)));
			}

			// Test the next three triggers
			for (int j = 0; j < 3; j++) {
				// The first add should trigger now
				counter++;
				assertTrue("Triggerpolicy with count of " + i
						+ " did not trigger at the expected pos " + counter + ".",
						triggerPolicy.notifyTrigger(tuples.get(j)));

				// the next i-1 adds should not trigger
				for (int k = 0; k < i - 1; k++) {
					counter++;
					assertFalse(triggerPolicy.notifyTrigger(tuples.get(k)));
				}
			}
		}
	}
}
