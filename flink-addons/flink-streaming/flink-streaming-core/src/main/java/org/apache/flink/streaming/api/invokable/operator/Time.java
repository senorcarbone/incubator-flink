package org.apache.flink.streaming.api.invokable.operator;


import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;

public class Time<DATA> implements NextGenWindowHelper<DATA> {

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
	public NextGenEvictionPolicy<DATA> toEvict() {
		return new NextGenTimeEvictionPolicy<DATA>(granularityInMillis(), new DefaultTimeStamp<DATA>());
	}

	@Override
	public NextGenTriggerPolicy<DATA> toTrigger() {
		return new NextGenTimeTriggerPolicy<DATA>(granularityInMillis(), new DefaultTimeStamp<DATA>());
	}

	public static <DATA> Time<DATA> of(int timeVal, TimeUnit granularity) {
		return new Time<DATA>(timeVal, granularity);
	}
	
	private long granularityInMillis(){
		return this.granularity.toMillis(this.timeVal);
	}

}
