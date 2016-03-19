package org.apache.flink.streaming.api.windowing.policy;


public class TumblingPolicyGroup<DATA> extends DeterministicPolicyGroup<DATA> {

	private boolean initiated = false;

	public TumblingPolicyGroup(DeterministicTriggerPolicy<DATA> trigger) {
		super(trigger, new DeterministicTumblingEvictionPolicy<>(0));
	}

	@Override
	public int getWindowEvents(DATA tuple) {
		
		if (getTrigger().notifyTrigger(tuple)) {
			return (((short) 1) << 16) + (short) 1;
		}

		if (!initiated) {
			return 1;
		}

		return 0;
	}
}
