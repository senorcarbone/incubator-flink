package org.apache.flink.streaming.api.windowing.policy;

import org.apache.flink.streaming.api.operators.windowing.PairPolicyGroup;

import java.io.Serializable;

public class PairSequencer implements Serializable{
	private PairPolicyGroup group;
	private int indx;
	private long currValue;

	public PairSequencer(PairPolicyGroup group) {
		this.group = group;
		reset();
	}

	public long getCurrValue() {
		return currValue;
	}

	public void proceed(long val) {
		if (val == currValue) {
			indx = (indx == 0) ? 1 : 0;
			currValue = (indx == 0) ? group.getPart1() : group.getPart2();
		} else if (val < currValue) {
			currValue = currValue - val;
		} else {
			throw new RuntimeException("problem");
		}
	}

	public PairPolicyGroup getGroup() {
		return group;
	}
	
	public void reset(){
		this.indx = 0;
		this.currValue = group.getPart1();
	}

}
