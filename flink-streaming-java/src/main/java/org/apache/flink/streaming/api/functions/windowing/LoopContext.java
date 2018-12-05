package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.List;

public class LoopContext<K> {
	
	final List<Long> context;
	final long superstep;
	final K key;
	private final StreamingRuntimeContext ctx;

	public LoopContext(List<Long> context, long superstep, K key, StreamingRuntimeContext ctx) {
		this.context = context;
		this.superstep = superstep;
		this.key = key;
		this.ctx = ctx;
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

	@Override
	public String toString() {
		return super.toString()+" :: [ctx: "+ context +", step: "+ superstep +", key: "+key+"]";
	}
}
