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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;

public class StreamReduce<IN> extends ChainableStreamOperator<IN, IN> {
	private static final long serialVersionUID = 1L;

	private static final String STATENAME = "reducerState";

	protected ReduceFunction<IN> reducer;
	private OperatorState<IN> currentValue;

	public StreamReduce(ReduceFunction<IN> reducer) {
		super(reducer);
		this.reducer = reducer;
		currentValue = null;
	}

	@Override
	public void run() throws Exception {
		while (isRunning && readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {

		if (currentValue.getState() != null) {
			currentValue.update(reducer.reduce(copy(currentValue.getState()), nextObject));
		} else {
			currentValue.update(nextObject);
		}
		collector.collect(currentValue.getState());

	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);

		if (runtimeContext.containsState(STATENAME)) {
			currentValue = (OperatorState<IN>) runtimeContext.getState(STATENAME);
		} else {
			currentValue = new OperatorState<IN>(null);
			runtimeContext.registerState(STATENAME, currentValue);
		}
	}
}
