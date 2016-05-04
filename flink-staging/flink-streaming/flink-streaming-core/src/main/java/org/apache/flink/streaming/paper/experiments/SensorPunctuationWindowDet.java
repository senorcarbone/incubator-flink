package org.apache.flink.streaming.paper.experiments;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTriggerPolicy;

import java.util.Random;

public class SensorPunctuationWindowDet implements DeterministicTriggerPolicy<Tuple4<Long, Long, Long, Integer>> {

	private int bitmask;
//	private int currentState = Integer.MAX_VALUE; //initial state is -1
	private Random rnd = new Random();
	private int counter = 0;
	private int nextChange = 0;

	private final static int min = 500;
	private final static int median = 6870;
	private final static int max = 33995;
	private final static int a = 1029;
	private final static int b = 14610;

	public SensorPunctuationWindowDet(int sensorIndex) {
		this.bitmask = (int) Math.pow(2, sensorIndex);
		computeNext();
	}

	@Override
	public double getNextTriggerPosition(double previousPosition) {
		//not used
		return 0;
	}

	private void computeNext(){
			double d = -3;
			while (d > 2.698 || d < -2.698) {
				d = rnd.nextGaussian();
			}
			if (Math.abs(d) < 0.6745) {
				if (d < 0) {
					nextChange = (int) (median - (median - a) / 0.6745 * (-d));  // 2nd quartile
				} else {
					nextChange = (int) (median + (b - median) / 0.6745 * d);  // 3rd quartile
				}
			} else {
				if (d < 0) {
					nextChange = (int) (a - (a - min) / (2.698 - 0.6745) * ((-d) - 0.6745));  // 1st quartile
				} else {
					nextChange = (int) (b + (max - b) / (2.698 - 0.6745) * (d - 0.6745));  // 4th quartile
				}
			}
	}

	@Override
	public boolean notifyTrigger(Tuple4<Long, Long, Long, Integer> datapoint) {

		if(counter == nextChange){
			computeNext();
			counter=0;
			return true;
		}
		counter ++;
		return false;

//		int sensorVal = datapoint.f3 & bitmask;
//		if (currentState == Integer.MAX_VALUE) {
//			currentState = sensorVal;
//			return false;
//		}
//
//		int tmp = currentState;
//		return (currentState = sensorVal) != tmp;
	}
}