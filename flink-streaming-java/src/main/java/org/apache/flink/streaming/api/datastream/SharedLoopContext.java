package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;

public interface SharedLoopContext<S> {
	
	 MapState<Long,S> getWindowLoopState(LoopContext loopCtx);
	 ValueState<S> getPersistentLoopState(LoopContext loopCtx);
	 
}
