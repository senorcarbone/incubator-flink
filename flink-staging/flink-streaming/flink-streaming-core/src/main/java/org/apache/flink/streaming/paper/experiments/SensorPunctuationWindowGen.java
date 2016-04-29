package org.apache.flink.streaming.paper.experiments;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

public class SensorPunctuationWindowGen implements TriggerPolicy<Tuple4<Long, Long, Long, Integer>> {


	private int bitmask;
	private int currentState = Integer.MAX_VALUE; //initial state is -1 

	public SensorPunctuationWindowGen(int sensorIndex) {
		this.bitmask = (int) Math.pow(2, sensorIndex);
	}

	@Override
	public boolean notifyTrigger(Tuple4<Long, Long, Long, Integer> datapoint) {
		int sensorVal = datapoint.f3 & bitmask;
		if (currentState == Integer.MAX_VALUE) {
			currentState = sensorVal;
			return false;
		}

		int tmp = currentState;
		return (currentState = sensorVal) != tmp;
	}

}
