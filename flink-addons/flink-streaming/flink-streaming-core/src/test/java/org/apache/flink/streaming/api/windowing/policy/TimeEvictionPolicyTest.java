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

import java.util.LinkedList;

import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimeEvictionPolicyTest {

	@Test
	public void timeEvictionTest() {
		// create some test data
		Integer[] times = { 1, 3, 4, 6, 7, 9, 14, 20, 21, 22, 30 };

		// create a timestamp
		@SuppressWarnings("serial")
		TimeStamp<Integer> timeStamp = new TimeStamp<Integer>() {

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}

			@Override
			public long getStartTime() {
				return 0;
			}

		};

		// test different granularity
		for (long granularity = 0; granularity < 31; granularity++) {
			// create policy
			EvictionPolicy<Integer> policy = new TimeEvictionPolicy<Integer>(granularity, timeStamp);

			// The trigger status should not effect the policy. Therefore, it's
			// value is changed after each usage.
			boolean triggered = false;

			// test by adding values
			LinkedList<Integer> buffer = new LinkedList<Integer>();
			for (int i = 0; i < times.length; i++) {

				int result = policy.notifyEviction(times[i], (triggered = !triggered),
						buffer.size());

				// handle correctness of eviction
				for (; result > 0 && !buffer.isEmpty(); result--) {
					if (buffer.getFirst() < times[i] - granularity) {
						buffer.removeFirst();
					} else {
						fail("The policy wanted to evict time " + buffer.getFirst()
								+ " while the current time was " + times[i]
								+ "and the granularity was " + granularity);
					}
				}

				// test that all required evictions have been done
				if (!buffer.isEmpty()) {
					assertTrue("The policy did not evict " + buffer.getFirst()
							+ " while the current time was " + times[i]
							+ "and the granularity was " + granularity,
							(buffer.getFirst() >= times[i] - granularity));
				}

			}
		}
	}

}
