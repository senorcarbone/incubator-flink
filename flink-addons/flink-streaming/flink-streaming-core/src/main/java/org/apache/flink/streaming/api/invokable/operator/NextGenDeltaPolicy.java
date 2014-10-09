package org.apache.flink.streaming.api.invokable.operator;

import java.util.LinkedList;
import java.util.List;

public class NextGenDeltaPolicy<DATA> implements NextGenTriggerPolicy<DATA>, NextGenEvictionPolicy<DATA> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = -7797538922123394967L;

	private NextGenDeltaFunction<DATA> deltaFuntion;
	private List<DATA> windowBuffer;
	private double threshold;
	private DATA triggerDataPoint;

	public NextGenDeltaPolicy(NextGenDeltaFunction<DATA> deltaFuntion, DATA init, double threshold) {
		this.deltaFuntion = deltaFuntion;
		this.windowBuffer = new LinkedList<DATA>();
		this.threshold = threshold;
	}

	@Override
	public boolean notifyTrigger(DATA datapoint) {
		if (deltaFuntion.getDelta(this.triggerDataPoint, datapoint) > this.threshold) {
			this.triggerDataPoint = datapoint;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize) {
		windowBuffer = windowBuffer.subList(windowBuffer.size() - bufferSize, bufferSize);
		int evictCount = 0;
		for (DATA bufferPoint : windowBuffer) {
			if (deltaFuntion.getDelta(bufferPoint, datapoint) >= this.threshold) {
				windowBuffer = windowBuffer.subList(evictCount, windowBuffer.size());
				break;
			}
			evictCount++;
		}
		return evictCount;
	}

}
