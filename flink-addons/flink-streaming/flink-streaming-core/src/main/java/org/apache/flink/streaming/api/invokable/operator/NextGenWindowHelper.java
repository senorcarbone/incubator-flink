package org.apache.flink.streaming.api.invokable.operator;


public interface NextGenWindowHelper<DATA> {

	public NextGenEvictionPolicy<DATA> toEvict();

	public NextGenTriggerPolicy<DATA> toTrigger();

}
