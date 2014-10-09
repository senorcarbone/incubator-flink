package org.apache.flink.streaming.api.invokable.operator;


public class Delta<DATA> implements NextGenWindowHelper<DATA> {


	private NextGenDeltaFunction<DATA> deltaFunction;
	private DATA initVal;
	private int threshold;


	public Delta(NextGenDeltaFunction<DATA> deltaFunction, DATA initVal, int threshold) {
		this.deltaFunction = deltaFunction;
		this.initVal = initVal;
		this.threshold = threshold;
	}

	@Override
	public NextGenEvictionPolicy<DATA> toEvict() {
		return new NextGenDeltaPolicy<DATA>(deltaFunction, initVal, threshold);
	}

	@Override
	public NextGenTriggerPolicy<DATA> toTrigger() {
		return new NextGenDeltaPolicy<DATA>(deltaFunction, initVal, threshold);
	}

	public static <DATA> Delta<DATA> of(NextGenDeltaFunction<DATA> deltaFunction, DATA initVal, int threshold) {
		return new Delta<DATA>(deltaFunction, initVal, threshold);
	}

}
