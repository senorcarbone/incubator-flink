package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.streaming.api.invokable.util.TimeStamp;

public class NextGenTimeTriggerPolicy<DATA> implements NextGenTriggerPolicy<DATA>{

	/**
	 * auto generated version id
	 */
	private static final long serialVersionUID = -5122753802440196719L;

	private long startTime;
	private long granularity;
	private TimeStamp<DATA> timestamp;
	
	public NextGenTimeTriggerPolicy(long granularity, TimeStamp<DATA> timestamp) {
		this.startTime=timestamp.getStartTime();
		this.timestamp=timestamp;
		this.granularity=granularity;
	}
	
	@Override
	public boolean notifyTrigger(DATA datapoint) {
		return nextWindow(timestamp.getTimestamp(datapoint));
	}
	
	private boolean nextWindow(long recordTime){
		if (recordTime < startTime + granularity) {
			return true;
		} else {
			startTime += granularity;
			return false;
		}
	}
	
}
