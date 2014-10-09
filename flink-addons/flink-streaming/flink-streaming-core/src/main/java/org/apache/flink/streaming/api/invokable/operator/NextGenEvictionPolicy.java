package org.apache.flink.streaming.api.invokable.operator;

import java.io.Serializable;

public interface NextGenEvictionPolicy<DATA> extends Serializable{

	/**
	 * Proves if and how many elements should be deleted from the
	 * element buffer. The eviction takes place after the trigger
	 * and after the call to the UDF but before the adding of the
	 * new datapoint.
	 *
	 * @param datapoint datapoint the data point which arrived
	 * @param triggered Information whether the UDF was triggered or not
	 * @param bufferSize
     * @return The number of elements to be deleted from the buffer
	 */
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize);
}
