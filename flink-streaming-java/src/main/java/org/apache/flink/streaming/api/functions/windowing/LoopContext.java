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

	public K getKey() {
		return key;
	}

	public List<Long> getContext() {
		return context;
	}

	public long getSuperstep() {
		return superstep;
	}

	public StreamingRuntimeContext getRuntimeContext() {
		return ctx;
	}

	public boolean hasLoopState() throws Exception {
		checkInitialization();
		return managedStateHandle.getWindowLoopState().contains(context.get(context.size() - 1));
	}

	private void checkInitialization() throws IllegalStateException{
		if (managedStateHandle == null)
			throw new IllegalStateException("Managed State not Initialized");
	}

	public S loopState() throws Exception {
		checkInitialization();
		return managedStateHandle.getWindowLoopState().get(context.get(context.size() - 1));
	}
	
	public void loopState(S newVal) throws Exception {
		checkInitialization();
		managedStateHandle.getWindowLoopState().put(context.get(context.size()-1), newVal);
		managedStateHandle.markActive(this.context, this.key);
	}

	public S persistentState() throws Exception {
		checkInitialization();
		return managedStateHandle.getPersistentLoopState().value();
	}

	public void persistentState(S newVal) throws Exception {
		checkInitialization();
		managedStateHandle.getPersistentLoopState().update(newVal);
		managedStateHandle.markActive(this.context, this.key);
	}

	@Override
	public String toString()  {
		try {
			return super.toString() + " :: [ctx: " + context + ", partition: "+getRuntimeContext().getIndexOfThisSubtask()+ ", step: " + superstep + ", key: " + key + ", STATE[ LOOP: "+ loopState() + ", PERSISTENT: " + persistentState()+"] ]";
		} catch (Exception e) {
			return super.toString() + " :: [ctx: " + context + ", partition: "+getRuntimeContext().getIndexOfThisSubtask()+ ", step: " + superstep + ", key: " + key + ", [UNINITIALIZED STATE] ]";
		}
	}
}
