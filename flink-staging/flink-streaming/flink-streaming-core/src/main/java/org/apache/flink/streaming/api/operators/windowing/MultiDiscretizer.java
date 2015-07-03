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

import java.util.HashMap;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.OptimizedWindowBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a merged discretization operator handling
 * discretizations from several queries.
 * 
 * The ids for queries are determined as follows: For queries with deterministic
 * policies: 0 to (NUM_DETERMINISTIC_QUERIES-1) For not deterministic queries:
 * NUM_DETERMINISTIC_QUERIES to (TOTAL_NUM_OF_QUERIES-1)
 * 
 * @param <IN>
 *            The type of input tuples handled by this operator
 */
@SuppressWarnings("unused")
public class MultiDiscretizer<IN> extends
		AbstractStreamOperator<Tuple2<Integer, IN>> implements
		OneInputStreamOperator<IN, Tuple2<Integer, IN>> {

	private static final Logger LOG = LoggerFactory
			.getLogger(MultiDiscretizer.class);

	private LinkedList<DeterministicPolicyGroup<IN>> deterministicPolicyGroups;
	private HashMap<Integer, LinkedList<Integer>> queryIdToWindowIds = new HashMap<Integer, LinkedList<Integer>>();
	private LinkedList<TriggerPolicy<IN>> triggerPolicies;
	private LinkedList<EvictionPolicy<IN>> evictionPolicies;
	private int[] bufferSizes;
	private boolean[] isActiveTrigger;
	private boolean[] isActiveEviction;
	private OptimizedWindowBuffer<IN> optimizedWindowBuffer;

	/**
	 * This constructor takes the policies of multiple queries.
	 * 
	 * @param deterministicPolicyGroups
	 *            The policy groups representing the deterministic queries
	 * @param notDeterministicTriggerPolicies
	 *            The trigger policies of the not deterministic queries. This
	 *            must be the same size as the eviction policy list!
	 * @param notDeterministicEvictionPolicies
	 *            The eviction policies of the not deterministic queries. This
	 *            must have the same size as the trigger policy list!
	 */
	public MultiDiscretizer(
			LinkedList<DeterministicPolicyGroup<IN>> deterministicPolicyGroups,
			LinkedList<TriggerPolicy<IN>> notDeterministicTriggerPolicies,
			LinkedList<EvictionPolicy<IN>> notDeterministicEvictionPolicies,
			ReduceFunction<IN> reduceFunction) {
		this.deterministicPolicyGroups = deterministicPolicyGroups;
		this.evictionPolicies = notDeterministicEvictionPolicies;
		this.triggerPolicies = notDeterministicTriggerPolicies;
		this.bufferSizes = new int[triggerPolicies.size()];
		this.isActiveTrigger = new boolean[triggerPolicies.size()];
		this.isActiveEviction = new boolean[evictionPolicies.size()];
		this.optimizedWindowBuffer = new OptimizedWindowBuffer<IN>(
				reduceFunction, !deterministicPolicyGroups.isEmpty(),
				!triggerPolicies.isEmpty());

		for (int i = 0; i < this.deterministicPolicyGroups.size(); i++) {
			queryIdToWindowIds.put(i, new LinkedList<Integer>());
		}

		for (int i = 0; i < triggerPolicies.size(); i++) {
			if (i != optimizedWindowBuffer.registerQuery()) {
				// the id which is returned from the buffer should be the same
				// as the one kept here.
				// Otherwise we throw an exception. For production use this
				// should be changed.
				throw new RuntimeException(
						"The returned registration id does not match the expected id");
			}
		}

		// Catch active policies
		for (int i = 0; i < isActiveEviction.length; i++) {

			// Active trigger
			isActiveTrigger[i] = triggerPolicies.get(i) instanceof ActiveTriggerPolicy;

			// Active eviction
			isActiveEviction[i] = evictionPolicies.get(i) instanceof ActiveEvictionPolicy;

			bufferSizes[i] = 0;
		}

		chainingStrategy = ChainingStrategy.ALWAYS;

	}

	@SuppressWarnings("unchecked")
	@Override
	public void processElement(IN tuple) throws Exception {
		// First handle the deterministic policies
		for (int i = 0; i < deterministicPolicyGroups.size(); i++) {
			int windowEvents = deterministicPolicyGroups.get(i)
					.getWindowEvents(tuple);
			// processWindowBegins
			for (int j = 0; j < (windowEvents >> 16); j++) {
				beginWindow(i);
			}
			// processWindowEnds
			for (int j = 0; j < (windowEvents & 0xFFFF); j++) {
				endWindow(i);
			}
		}

		// Now handle the not deterministic queries
		for (int i = 0; i < triggerPolicies.size(); i++) {

			// Do pre-notification for active triggers
			if (isActiveTrigger[i]) {
				Object[] preNotificationTuples = ((ActiveTriggerPolicy) triggerPolicies
						.get(i)).preNotifyTrigger(tuple);
				for (Object preNotificationTuple : preNotificationTuples) {
					if (isActiveEviction[i]) {
						evict(i, ((ActiveEvictionPolicy) evictionPolicies
								.get(i)).notifyEvictionWithFakeElement(
								preNotificationTuple, bufferSizes[i]));
					}
					emitWindow(i);
				}
			}

			// Do regular notification
			if (triggerPolicies.get(i).notifyTrigger(tuple)) {
				emitWindow(i);
				evict(i,
						evictionPolicies.get(i).notifyEviction(tuple, true,
								bufferSizes[i]));
			} else {
				evict(i,
						evictionPolicies.get(i).notifyEviction(tuple, false,
								bufferSizes[i]));
			}

		}

		// Finally add the current tuple to the buffer
		store(tuple);
	}

	/**
	 * Send a window begin marker for a deterministic policy group
	 * 
	 * @param queryId
	 *            the query this marker belongs to. Remark; deterministic and
	 *            not deterministic policies are numbered separately!
	 */
	private void beginWindow(int queryId) {
		int windowId = optimizedWindowBuffer.startWindow();
		queryIdToWindowIds.get(queryId).add(windowId);
	}

	/**
	 * Send a window end marker for a deterministic policy group
	 * 
	 * The returned ids for queries are determined as follows: For queries with
	 * deterministic policies: 0 to (NUM_DETERMINISTIC_QUERIES-1) For not
	 * deterministic queries: NUM_DETERMINISTIC_QUERIES to
	 * (TOTAL_NUM_OF_QUERIES-1) Internally, this class has separated ids for
	 * deterministic and not deterministic queries.
	 * 
	 * @param queryId
	 *            the query this marker belongs to Remark; deterministic and not
	 *            deterministic policies are numbered separately!
	 */
	private void endWindow(int queryId) throws Exception {
		LOG.info("EndWindow id: {}", queryId);
		output.collect(new Tuple2<Integer, IN>(queryId, optimizedWindowBuffer
				.endWindow(queryIdToWindowIds.get(queryId).getFirst())));

		queryIdToWindowIds.get(queryId).removeFirst();
	}

	/**
	 * Adds the given tuple to the buffer
	 * 
	 * @param tuple
	 *            the input tuple
	 */
	private void store(IN tuple) throws Exception {
		for (int i = 0; i < bufferSizes.length; i++) {
			bufferSizes[i] = bufferSizes[i] + 1;
		}
		optimizedWindowBuffer.store(tuple);
	}

	/**
	 * Sends a window end even for not deterministic policies.
	 * 
	 * The returned ids for queries are determined as follows: For queries with
	 * deterministic policies: 0 to (NUM_DETERMINISTIC_QUERIES-1) For not
	 * deterministic queries: NUM_DETERMINISTIC_QUERIES to
	 * (TOTAL_NUM_OF_QUERIES-1) Internally, this class has separated ids for
	 * deterministic and not deterministic queries.
	 * 
	 * @param queryId
	 *            the query this marker belongs to Remark; deterministic and not
	 *            deterministic policies are numbered separately!
	 */
	private void emitWindow(int queryId) throws Exception {
		LOG.info("EmitWindow id: {}", queryId);
		output.collect(new Tuple2<Integer, IN>(queryId
				+ deterministicPolicyGroups.size(), optimizedWindowBuffer
				.emitWindow(queryId)));
	}

	/**
	 * Sends a eviction request for a not deterministic query
	 * 
	 * @param queryId
	 *            the query this marker belongs to Remark; deterministic and not
	 *            deterministic policies are numbered separately!
	 * @param n
	 *            the number of tuple to delete from the buffer
	 */
	private void evict(int queryId, int n) {
		if (n > 0) {
			bufferSizes[queryId] = bufferSizes[queryId] - n;
		}
		optimizedWindowBuffer.evict(n, queryId);
	}
}
