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

package org.apache.flink.streaming.api.operators;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;

public class StreamGroupedReduce<IN> extends StreamReduce<IN> {
	private static final long serialVersionUID = 1L;
	private static final String STATENAME = "groupReducerState";

	private KeySelector<IN, ?> keySelector;
	private OperatorState<Map<Object, IN>> currentState;

	public StreamGroupedReduce(ReduceFunction<IN> reducer, KeySelector<IN, ?> keySelector) {
		super(reducer);
		this.keySelector = keySelector;
	}

	@Override
	protected void callUserFunction() throws Exception {
		Object key = keySelector.getKey(nextObject);

		Map<Object, IN> values = currentState.getState();

		IN currentValue = values.get(key);
		if (currentValue != null) {
			IN reduced = reducer.reduce(copy(currentValue), nextObject);
			values.put(key, reduced);
			collector.collect(reduced);
		} else {
			values.put(key, nextObject);
			collector.collect(nextObject);
		}
		currentState.update(values);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);

		if (runtimeContext.containsState(STATENAME)) {
			currentState = (OperatorState<Map<Object, IN>>) runtimeContext.getState(STATENAME);
		} else {
			currentState = new OperatorState<Map<Object, IN>>(new HashMap<Object, IN>());
			runtimeContext.registerState(STATENAME, currentState);
		}
	}

}
