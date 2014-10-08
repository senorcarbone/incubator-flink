package org.apache.flink.streaming.api.invokable.operator;

import java.io.Serializable;

public interface NextGenDeltaFunction<DATA> extends Serializable {

	public int getDelta(DATA oldDataPoint, DATA newDataPoint);
	
}
