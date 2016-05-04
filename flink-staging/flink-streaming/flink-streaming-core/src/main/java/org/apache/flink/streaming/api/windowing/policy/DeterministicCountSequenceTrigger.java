package org.apache.flink.streaming.api.windowing.policy;


import java.util.List;

/**
 * As the name hints this is a deterministic count-based trigger policy that cycles through a given sequence of count
 * sequences. This can be particularly useful for certain pre-computed types of pre-aggregations such as pairs.
 *
 * @param <IN>
 */
public class DeterministicCountSequenceTrigger<IN> implements CloneableTriggerPolicy<IN>, DeterministicTriggerPolicy<IN> {

	private final List<Long> sequence;
	private int curIndex;
	private long counter;
	private int startValue;
	private int posInSeq;

	public DeterministicCountSequenceTrigger(List<Long> sequence, int startValue) {
		this.sequence = sequence;
		this.curIndex = 0;
		this.startValue = startValue;
	}

	@Override
	public CloneableTriggerPolicy<IN> clone() {
		return new DeterministicCountSequenceTrigger<IN>(sequence, startValue);
	}

	@Override
	public double getNextTriggerPosition(double previousTriggerPosition) {
		double ret = 0;

		if (previousTriggerPosition < 0) {
			ret = getCurrentMax() + startValue;
		} else {
			ret = previousTriggerPosition + sequence.get(posInSeq);
		}

		posInSeq = (posInSeq + 1) % sequence.size();
		return ret;
//		if (previousTriggerPosition < 0) {
//			return getCurrentMax() + startValue;
//		} else {
//			return previousTriggerPosition + getCurrentMax();
//		}
	}

	@Override
	public boolean notifyTrigger(IN datapoint) {
		if (counter >= getCurrentMax()) {
			counter = 1;
			proceedMax();
			return true;
		} else {
			counter++;
			return false;
		}
	}

	public List<Long> getSequence() {
		return sequence;
	}

	private void proceedMax() {
		curIndex = (curIndex + 1) % sequence.size();
	}

	private Long getCurrentMax() {
		return sequence.get(curIndex);
	}
}
