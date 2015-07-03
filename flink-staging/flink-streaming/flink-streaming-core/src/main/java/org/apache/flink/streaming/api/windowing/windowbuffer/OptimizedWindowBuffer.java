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

package org.apache.flink.streaming.api.windowing.windowbuffer;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;

public class OptimizedWindowBuffer<T> implements Serializable {

	private Map<Integer, Integer> windowToStartId = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> queryToStartId = new HashMap<Integer, Integer>();
	private LinkedList<Integer> preAggregationStartIds = new LinkedList<Integer>();
	private LinkedList<T> preAggregations = new LinkedList<T>();
	private LinkedList<T> individualTuples = new LinkedList<T>();
	private int nextSeqId = 0;
	private int nextQueryId = 0;
	private int nextWindowId = 0;
	private boolean keepIndvidualTuples;
	private boolean doPreAggregation;
	private boolean startNewPreAggregation = true;
	private ReduceFunction<T> reduceFunction;

	public OptimizedWindowBuffer(ReduceFunction<T> reduceFunction,
			boolean doPreAggregation, boolean keepIndvidualTuples) {
		this.keepIndvidualTuples = keepIndvidualTuples;
		this.doPreAggregation = doPreAggregation;
		this.reduceFunction = reduceFunction;
	}

	public void store(final T element) throws Exception {
		// Handle individual tuples
		if (keepIndvidualTuples) {
			individualTuples.add(element);
		}

		// Handle pre-aggregation
		if (doPreAggregation) {
			// If border start new pre-aggregation
			if (startNewPreAggregation || preAggregations.isEmpty()) {
				preAggregationStartIds.add(nextSeqId);
				preAggregations.add(element);
				this.startNewPreAggregation = false;
			} else {
				T newValue = this.reduceFunction.reduce(
						preAggregations.getLast(), element);
				preAggregations.removeLast();
				preAggregations.add(newValue);
			}
		}

		// increase sequence id
		this.nextSeqId++;

		// handle overflow of sequence id
		if (nextSeqId == Integer.MAX_VALUE) {
			handleSequenceIdOverflow();
		}
	}

	public void evict(int n, int queryId) {
		queryToStartId.put(queryId, queryToStartId.get(queryId) + n);
		deleteExpired();
	}

	public T emitWindow(int queryId) throws Exception {
		int startId = queryToStartId.get(queryId);
		Iterator<Integer> descendingIterator = preAggregationStartIds
				.descendingIterator();
		T result = null;
		int counter = preAggregationStartIds.size() - 1;
		int lastPreAggStart = nextSeqId;

		// Iteration threw the pre-aggregations
		if (doPreAggregation) {
			while (descendingIterator.hasNext()) {
				int preAggStart = descendingIterator.next();
				if (preAggStart >= startId) {
					lastPreAggStart = preAggStart;
					if (result == null) {
						result = preAggregations.get(counter);
					} else {
						result = this.reduceFunction.reduce(
								preAggregations.get(counter), result);
					}
					counter--;
				} else {
					break;
				}
			}
		}

		// Filling the gap from window start to start of the used pre
		// aggregation
		int start = individualTuples.size() - (nextSeqId - startId);
		int end = individualTuples.size() - (nextSeqId - lastPreAggStart);

		for (int i = start; i < end; i++) {
			if (result == null) {
				result = individualTuples.get(i);
			} else {
				result = this.reduceFunction.reduce(result,
						individualTuples.get((int) i));
			}
		}

		return result;
	}

	public int startWindow() {
		this.windowToStartId.put(this.nextWindowId, this.nextSeqId);
		this.startNewPreAggregation = true;
		return nextWindowId++;
	}

	public T endWindow(int windowId) throws Exception {
		long start = this.windowToStartId.get(windowId);
		this.windowToStartId.remove(windowId);

		// catch empty windows
		if (start == nextSeqId) {
			return null;
		}

		// reduce pre-aggregations to final result
		T result = preAggregations.getLast();
		int counter = preAggregations.size() - 2; // last already fetched
		while (counter >= 0 && preAggregationStartIds.get(counter) >= start) {
			result = this.reduceFunction.reduce(preAggregations.get(counter),
					result);
			counter--;
		}

		deleteExpired();

		return result;
	}

	public int registerQuery() {
		this.queryToStartId.put(nextQueryId, nextSeqId);
		return this.nextQueryId++;
	}

	public void deregisterQuery(int id) {
		this.queryToStartId.remove(id);
		deleteExpired();
	}

	public OptimizedWindowBuffer<T> clone() {
		// TODO If needed: Implement this!
		return null;
	}

	private void handleSequenceIdOverflow() {
		// TODO this class will fail in case more than MAX_INT tuples arrive
		throw new RuntimeException(
				"The sequence id reached the limit given by the type long!");
	}

	private void deleteExpired() {
		// Collect current eviction position
		Collection<Integer> queryBufferStarts = queryToStartId.values();
		Collection<Integer> windowBufferStarts = windowToStartId.values();

		// What is the oldest required tuple?
		int minimum = Integer.MAX_VALUE;
		for (int current : queryBufferStarts) {
			if (current < minimum) {
				minimum = current;
			}
		}
		for (int current : windowBufferStarts) {
			if (current < minimum) {
				minimum = current;
			}
		}

		// Delete all older than the minimum
		while (!preAggregations.isEmpty()
				&& preAggregationStartIds.getFirst() < minimum) {
			preAggregationStartIds.removeFirst();
			preAggregations.removeFirst();
		}

		if (!individualTuples.isEmpty()) {
			long numToDelete = individualTuples.size() - (nextSeqId - minimum);
			for (int i = 0; i < numToDelete; i++) {
				individualTuples.removeFirst();
			}
		}
	}

}
