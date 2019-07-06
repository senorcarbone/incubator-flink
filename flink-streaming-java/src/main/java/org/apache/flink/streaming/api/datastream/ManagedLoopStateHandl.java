package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

import java.util.List;

public interface ManagedLoopStateHandl<K, S> {

	/**
	 * State Handle for temporary in-loop state
	 * @return
	 */
	 MapState<Long,S> getWindowLoopState();

	/**
	 * State Handle for persistent state with cross-window access
	 * @return
	 */
	ValueState<S> getPersistentLoopState();

	/**
	 * Interface for informing the runtime about active keys within an iterative process
	 * @param key
	 */
	void markActive(List<Long> context, K key);
	 
}
