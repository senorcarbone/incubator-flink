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

package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class NextGenWindowingInvokable<IN> extends StreamInvokable<IN, Tuple2<IN, String[]>> {

	/**
	 * Auto-generated serial version UID
	 */
	private static final long serialVersionUID = -8038984294071650730L;

	private static final Logger LOG = LoggerFactory.getLogger(NextGenWindowingInvokable.class);

	private LinkedList<NextGenTriggerPolicy<IN>> triggerPolicies;
	private LinkedList<NextGenEvictionPolicy<IN>> evictionPolicies;
	private LinkedList<IN> buffer = new LinkedList<IN>();
	private LinkedList<NextGenTriggerPolicy<IN>> currentTriggerPolicies = new LinkedList<NextGenTriggerPolicy<IN>>();
	private ReduceFunction<IN> reducer;

	public NextGenWindowingInvokable(ReduceFunction<IN> userFunction,
			LinkedList<NextGenTriggerPolicy<IN>> triggerPolicies,
			LinkedList<NextGenEvictionPolicy<IN>> evictionPolicies) {
		super(userFunction);

		this.reducer = userFunction;
		this.triggerPolicies = triggerPolicies;
		this.evictionPolicies = evictionPolicies;
	}

	@Override
	protected void immutableInvoke() throws Exception {
		// Prevent empty data streams
		if ((reuse = recordIterator.next(reuse)) == null) {
			throw new RuntimeException("DataStream must not be empty");
		}

		while (reuse != null) {

			// Remember if a trigger occurred
			boolean isTriggered = false;

			// Process the triggers (in case of multiple triggers compute only
			// once!)
			for (NextGenTriggerPolicy<IN> triggerPolicy : triggerPolicies) {
				if (triggerPolicy.notifyTrigger(reuse.getObject())) {
					currentTriggerPolicies.add(triggerPolicy);
				}
			}

			if (!currentTriggerPolicies.isEmpty()) {
				// emit
				callUserFunctionAndLogException();

				// clear the flag collection
				currentTriggerPolicies.clear();

				// remember trigger
				isTriggered = true;
			}

			// Process the evictions and take care of double evictions
			// In case there are multiple eviction policies present,
			// only the one with the highest return value is recognized.
			int currentMaxEviction = 0;
			for (NextGenEvictionPolicy<IN> evictionPolicy : evictionPolicies) {
				// use temporary variable to prevent multiple calls to
				// notifyEviction
				int tmp = evictionPolicy.notifyEviction(reuse.getObject(), isTriggered,
						buffer.size());
				if (tmp > currentMaxEviction) {
					currentMaxEviction = tmp;
				}
			}
			for (int i = 0; i < currentMaxEviction; i++) {
				try {
					buffer.removeFirst();
				} catch (NoSuchElementException e) {
					// In case no more elements are in the buffer:
					// Prevent failure and stop deleting.
					break;
				}
			}

			// Add the current element to the buffer
			buffer.add(reuse.getObject());

			// Recreate the reuse-StremRecord object and load next StreamRecord
			resetReuse();
			reuse = recordIterator.next(reuse);
		}

		// finally trigger the buffer.
		if (!buffer.isEmpty()) {
			currentTriggerPolicies.clear();
			for (NextGenTriggerPolicy<IN> policy : triggerPolicies) {
				currentTriggerPolicies.add(policy);
			}
			callUserFunctionAndLogException();
		}

	}

	@Override
	protected void mutableInvoke() throws Exception {
		if (LOG.isInfoEnabled()) {
			LOG.info("There is currently no mutable implementation of this operator. Immutable version is used.");
		}
		immutableInvoke();
	}

	@Override
	protected void callUserFunction() throws Exception {
		Iterator<IN> reducedIterator = buffer.iterator();
		IN reduced = null;

		while (reducedIterator.hasNext() && reduced == null) {
			reduced = reducedIterator.next();
		}

		while (reducedIterator.hasNext()) {
			IN next = reducedIterator.next();
			if (next != null) {
				reduced = reducer.reduce(reduced, next);
			}
		}
		if (reduced != null) {
			String[] tmp = new String[currentTriggerPolicies.size()];
			for (int i = 0; i < tmp.length; i++) {
				tmp[i] = currentTriggerPolicies.get(i).toString();
			}
			collector.collect(new Tuple2<IN, String[]>(reduced, tmp));
		}
	}

}
