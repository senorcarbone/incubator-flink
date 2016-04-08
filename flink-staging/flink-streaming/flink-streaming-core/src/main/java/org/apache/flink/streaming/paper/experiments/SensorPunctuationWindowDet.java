package org.apache.flink.streaming.paper.experiments;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTriggerPolicy;

public class SensorPunctuationWindowDet implements DeterministicTriggerPolicy<Tuple4<Long, Long, Long, Integer>> {

	private int bitmask;
	private int currentState = Integer.MAX_VALUE; //initial state is -1 

	public SensorPunctuationWindowDet(int sensorIndex) {
		this.bitmask = (int) Math.pow(2, sensorIndex);
	}

	@Override
	public double getNextTriggerPosition(double previousPosition) {
		//not used
		return 0;
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