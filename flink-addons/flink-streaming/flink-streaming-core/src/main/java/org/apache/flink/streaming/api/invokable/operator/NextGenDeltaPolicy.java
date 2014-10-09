package org.apache.flink.streaming.api.invokable.operator;

import java.util.LinkedList;
import java.util.List;

public class NextGenDeltaPolicy<DATA> implements NextGenTriggerPolicy<DATA>, NextGenEvictionPolicy<DATA> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = -7797538922123394967L;

	private static final int DEFAULT_DELETE_ON_EVICTION = 1;

	private NextGenDeltaFunction<DATA> deltaFuntion;
	private List<DATA> windowBuffer;
	private int threshold;
	private int deleteOnEviction;
	private DATA triggerDataPoint;

	public NextGenDeltaPolicy(NextGenDeltaFunction<DATA> deltaFuntion, DATA init, int threshold) {
		this(deltaFuntion, init, threshold, DEFAULT_DELETE_ON_EVICTION);
	}

	public NextGenDeltaPolicy(NextGenDeltaFunction<DATA> deltaFuntion, DATA init, int threshold, int deleteOnEviction) {
		this.deltaFuntion = deltaFuntion;
		this.windowBuffer = new LinkedList<DATA>();
		this.threshold = threshold;
		this.deleteOnEviction = deleteOnEviction;
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
