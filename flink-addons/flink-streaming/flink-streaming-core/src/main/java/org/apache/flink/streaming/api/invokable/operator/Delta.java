package org.apache.flink.streaming.api.invokable.operator;


public class Delta<DATA> implements NextGenWindowHelper<DATA> {


	private NextGenDeltaFunction<DATA> deltaFunction;
	private DATA initVal;
	private int threshold;

	@Override
	public NextGenEvictionPolicy<DATA> toEvict() {
		return new NextGenDeltaPolicy<DATA>(deltaFunction, initVal, threshold);
	}

	@Override
	public NextGenTriggerPolicy<DATA> toTrigger() {
		return new NextGenDeltaPolicy<DATA>(deltaFunction, initVal, threshold);
	}

}
