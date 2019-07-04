package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

public interface ManagedLoopStateHandl<S> {
	
	 MapState<Long,S> getWindowLoopState();
	 ValueState<S> getPersistentLoopState();
	 
}
