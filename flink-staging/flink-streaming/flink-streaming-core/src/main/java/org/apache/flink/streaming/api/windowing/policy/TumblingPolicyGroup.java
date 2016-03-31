package org.apache.flink.streaming.api.windowing.policy;


public class TumblingPolicyGroup<DATA> extends DeterministicPolicyGroup<DATA> {

	private boolean initiated = false;

	public TumblingPolicyGroup(DeterministicTriggerPolicy<DATA> trigger) {
		super(trigger, new DeterministicTumblingEvictionPolicy<>(0));
	}

	@Override
	public int getWindowEvents(DATA tuple) {
		
		if (getTrigger().notifyTrigger(tuple)) {
			return 65537;  // window starts and ends
		}

		if (!initiated) {
			initiated = true;
			return 65536;  // window starts
		}

		return 0; // no starts or ends
	}
}
