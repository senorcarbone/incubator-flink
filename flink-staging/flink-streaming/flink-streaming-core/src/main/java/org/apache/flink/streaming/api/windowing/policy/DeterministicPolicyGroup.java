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

import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.extractor.FieldFromTuple;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * A policy group wraps around several deterministig windowing policies which
 * operate on the same field of the input tuples.
 * 
 * @param <DATA>
 *            the type of input tuples handled by this policy group
 */
public class DeterministicPolicyGroup<DATA> implements Serializable {

	private DeterministicTriggerPolicy<DATA> trigger;
	private DeterministicEvictionPolicy<DATA> eviction;
	private Extractor<DATA, Double> fieldExtractor;
	private LinkedList<Double> windowStartLookahead = new LinkedList<Double>();
	private LinkedList<Double> windowEndLookahead = new LinkedList<Double>();
	private double latestTriggerPosition = Long.MIN_VALUE;
	private int currentBufferSize=0;

	/**
	 * This constructer sets up the policy group in case Tuple-types are used.
	 * NOTE: If DATA is not instance of Tuple, the usage of this constructor
	 * will cause an exception at runtime.
	 * 
	 * @param trigger
	 *            The deterministic trigger policy of this group
	 * @param eviction
	 *            The deterministic eviction policy of this group
	 * @param keyFieldId
	 *            The id of the field in the input tuple on which the policies
	 *            operate
	 */
	@SuppressWarnings("unchecked, unused")
	public DeterministicPolicyGroup(DeterministicTriggerPolicy<DATA> trigger,
			DeterministicEvictionPolicy<DATA> eviction, int keyFieldId) {
		this(trigger, eviction);
		this.fieldExtractor = new FieldFromTuple(keyFieldId);
	}

	/**
	 * This constructor sets up the policy group. It can handle arbitrary input
	 * types using the provided key selector.
	 * 
	 * @param trigger
	 *            The deterministic trigger policy of this group
	 * @param eviction
	 *            The deterministic eviction policy of this group
	 * @param fieldExtractor
	 *            A key selector which selects the field from the input on which
	 *            the policies operate
	 */
	@SuppressWarnings("unchecked, unused")
	public DeterministicPolicyGroup(DeterministicTriggerPolicy<DATA> trigger,
			DeterministicEvictionPolicy<DATA> eviction, Extractor fieldExtractor) {
		this(trigger, eviction);
		this.fieldExtractor = fieldExtractor;
	}

	/**
	 * This constructor can only be used if the type of DATA is Double already.
	 * No extractor will be applied if this constructor is used. (Just to
	 * prevent duplicated code)
	 * 
	 * @param trigger
	 *            The deterministic trigger policy of this group
	 * @param eviction
	 *            The deterministic eviction policy of this group
	 */
	public DeterministicPolicyGroup(DeterministicTriggerPolicy<DATA> trigger,
			DeterministicEvictionPolicy<DATA> eviction) {
		this.trigger = trigger;
		this.eviction = eviction;
	}

	/**
	 * This method is used to obtain the window events which are caused by the
	 * arrived of the tuple wich is given as parameter.
	 * 
	 * @param tuple
	 *            The currently arrives input tuple
	 * @return The window events caused by the arrival of this tuple: 0 if no
	 *         event occurs otherwise number of opened windows in the first
	 *         16bit, number of closed windows in the last 16bit. To get the
	 *         window begin counter use: (result >> 16) To get the window end
	 *         counter use: (result & 0xFFFF)
	 */
	public int getWindowEvents(DATA tuple) {

		double position;

		// Call the regular notification methods of the policies
		// This is required to allow them to react on date characteristics.
		boolean triggered = this.trigger.notifyTrigger(tuple);
		int numToEvict = this.eviction.notifyEviction(tuple,triggered,currentBufferSize);
		//remember/update current buffer size
		if (currentBufferSize-numToEvict<0){
			currentBufferSize=0;
		} else {
			currentBufferSize-=numToEvict;
		}
		currentBufferSize++;
	
		// Extract the required field from the tuple
		if (fieldExtractor == null) {
			// If an exception is thrown here, neither DATA is instance of
			// Double nor a correct extractor has been set.
			position = (Double) tuple;
		} else {
			position = this.fieldExtractor.extract(tuple);
		}

		// Extend the look ahead such that we know a complete window beyond the
		// position of the current tuple
		while (windowStartLookahead.isEmpty()
				|| windowStartLookahead.getLast() <= position) {
			windowEndLookahead.add(latestTriggerPosition = trigger
					.getNextTriggerPosition(latestTriggerPosition));
			windowStartLookahead.add(eviction
					.getLowerBorder(latestTriggerPosition));
		}

		// Calculate the number of window begins
		short windowBeginCounter = 0;
		while (!windowStartLookahead.isEmpty()
				&& windowStartLookahead.getFirst() <= position) {
			windowStartLookahead.removeFirst();
			windowBeginCounter++;
		}

		// Calculate the number of window ends
		short windowEndCounter = 0;
		while (!windowEndLookahead.isEmpty()
				&& windowEndLookahead.getFirst() <= position) {
			windowEndLookahead.removeFirst();
			windowEndCounter++;
		}

		// Combine and return counter
		return (windowBeginCounter << 16) + windowEndCounter;
	}

}
