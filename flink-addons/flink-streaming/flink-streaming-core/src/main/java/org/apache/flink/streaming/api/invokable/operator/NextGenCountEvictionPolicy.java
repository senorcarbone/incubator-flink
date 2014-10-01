package org.apache.flink.streaming.api.invokable.operator;

public class NextGenCountEvictionPolicy<IN,FLAG> implements NextGenPolicy<IN, FLAG> {

	private static final int DEFAULT_START_VALUE=0;
	
	private int counter;
	private int max;
	private FLAG flag;
	
	public NextGenCountEvictionPolicy(FLAG flag, int max) {
		this(flag, max, DEFAULT_START_VALUE);
	}
	
	public NextGenCountEvictionPolicy(FLAG flag, int max, int startValue) {
		this.max=max;
		this.counter=startValue;
		this.flag=flag;
	}
	
	@Override
	public boolean addDataPoint(IN datapoint) {
		if (counter==max){
			//The current data point will be part of the next window!
			//Therefore counter needs to be set to one already.
			//TODO think about introducing different strategies for eviction:
			//     1) including last data point: Better/faster for count eviction
			//     2) excluding last data point: Essentially required for time based eviction and delta rules
			counter=1;
			return true;
		} else {
			counter++;
			return false;
		}
	}

	@Override
	public FLAG getFlag() {
		return flag;
	}

}
