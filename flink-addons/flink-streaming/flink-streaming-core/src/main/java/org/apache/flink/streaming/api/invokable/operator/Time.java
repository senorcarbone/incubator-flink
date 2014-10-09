package org.apache.flink.streaming.api.invokable.operator;


import java.util.concurrent.TimeUnit;

public class Time implements NextGenWindowHelper {

	private int timeVal;
	private TimeUnit granularity;

	public Time(int timeVal, TimeUnit granularity) {
		this.timeVal = timeVal;
		this.granularity = granularity;
	}

	public Time(int timeVal) {
		this(timeVal, TimeUnit.SECONDS);
	}

	@Override
	public NextGenEvictionPolicy<?> toEvict() {
		//TODO
		throw new UnsupportedOperationException("time eviction not implemented");
	}

	@Override
	public NextGenTriggerPolicy<?> toTrigger() {
		//TODO
		throw new UnsupportedOperationException("time triggering not implemented");
	}

	public static Time of(int timeVal, TimeUnit granularity) {
		return new Time(timeVal, granularity);
	}

}
