/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.streaming.api.datastream.ManagedLoopStateHandl;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.List;

public class LoopContext<K, S> {

	final List<Long> context;
	final long superstep;
	final K key;
	private final StreamingRuntimeContext ctx;
	private ManagedLoopStateHandl<K, S> managedStateHandle;

	public LoopContext(List<Long> context, long superstep, K key, StreamingRuntimeContext ctx, ManagedLoopStateHandl<K, S> managedStateHandle) {
		this.context = context;
		this.superstep = superstep;
		this.key = key;
		this.ctx = ctx;
		this.managedStateHandle = managedStateHandle;
	}

	/**
	 * The current key on which computation is invoked  
	 * @return 
	 */
	public K getKey() {
		return key;
	}

	/**
	 * The encapsulating context identifier where computation is invoked
	 * @return
	 */
	public List<Long> getContext() {
		return context;
	}

	/**
	 * The current superstep of the computation, i.e. progress identifier in current scope 
	 * @return
	 */
	public long getSuperstep() {
		return superstep;
	}
	
	public StreamingRuntimeContext getRuntimeContext() {
		return ctx;
	}

	/**
	 * Checks if there is a managed state entry for loop state in current key under current context 
	 * @return true if temporary loop state exists in current context for key
	 * @throws Exception
	 */
	public boolean hasLoopState() throws Exception {
		checkInitialization();
		return managedStateHandle.getWindowLoopState().contains(context.get(context.size() - 1));
	}

	/**
	 * Used for sanity checks when an iterative stream application has been initiated
	 * @throws IllegalStateException
	 */
	private void checkInitialization() throws IllegalStateException{
		if (managedStateHandle == null)
			throw new IllegalStateException("Managed State not Initialized");
	}

	/**
	 * Returns temporary in-loop state for current key and encapsulating context.
	 * This managed state entry is active only during a window iteration and gets garbage collected
	 * when the iteration has been finalized for the given key.
	 * @return stored loop state (null if empty)
	 * @throws Exception
	 */
	public S loopState() throws Exception {
		checkInitialization();
		return managedStateHandle.getWindowLoopState().get(context.get(context.size() - 1));
	}

	/**
	 * Sets temporary in-loop state for current key and encapsulating context to the given value.
	 * This managed state entry is active only during a window iteration and gets garbage collected
	 * when the iteration has been finalized for the given key.
	 * @param newVal
	 * @throws Exception
	 */
	public void loopState(S newVal) throws Exception {
		checkInitialization();
		managedStateHandle.getWindowLoopState().put(context.get(context.size()-1), newVal);
		managedStateHandle.markActive(this.context, this.key);
	}

	/**
	 * Returns persistent state for current key. 
	 * This managed state entry is shared among all active contexts (loop computations)
	 * @return stored persistent state (null if empty)
	 * @throws Exception
	 */
	public S persistentState() throws Exception {
		checkInitialization();
		return managedStateHandle.getPersistentLoopState().value();
	}

	/**
	 * Sets persistent state for current key to the given value.
	 * This managed state entry is shared among all active contexts (loop computations)
	 * @param newVal
	 * @throws Exception
	 */
	public void persistentState(S newVal) throws Exception {
		checkInitialization();
		managedStateHandle.getPersistentLoopState().update(newVal);
		managedStateHandle.markActive(this.context, this.key);
	}

	@Override
	public String toString()  {
		String stepString = (superstep == Long.MAX_VALUE) ? "FINAL" : String.valueOf(superstep);
		try {
			return super.toString() + " :: [ctx: " + context + ", partition: "+getRuntimeContext().getIndexOfThisSubtask()+ ", step: " + stepString + ", key: " + key + ", STATE[ LOOP: "+ loopState() + ", PERSISTENT: " + persistentState()+"] ]";
		} catch (Exception e) {
			return super.toString() + " :: [ctx: " + context + ", partition: "+getRuntimeContext().getIndexOfThisSubtask()+ ", step: " + stepString + ", key: " + key + ", [UNINITIALIZED STATE] ]";
		}
	}
}
