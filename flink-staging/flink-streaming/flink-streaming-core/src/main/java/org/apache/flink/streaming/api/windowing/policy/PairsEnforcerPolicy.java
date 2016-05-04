package org.apache.flink.streaming.api.windowing.policy;


import org.apache.flink.streaming.api.operators.windowing.PairPolicyGroup;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * As the name hints this is a deterministic count-based trigger policy that cycles through a given sequence of count
 * sequences. This can be particularly useful for certain pre-computed types of pre-aggregations such as pairs.
 *
 * @param <IN>
 */
public class PairsEnforcerPolicy<IN> implements CloneableTriggerPolicy<IN>, DeterministicTriggerPolicy<IN>, DeterministicEvictionPolicy<IN> {

	private final List<PairPolicyGroup<IN>> policies;
	private List<PairSequencer> sequencers = new ArrayList<>();
	private BigInteger currentSize;

	private long counter;
	private int startValue;
	private BigInteger lcm;

	private int tmp2Counter = 0;
	private int tmpCounter = 0;

	private LinkedList<Long> sequence = new LinkedList<>();
	private int lastChunk;

	public PairsEnforcerPolicy(List<PairPolicyGroup<IN>> policies, int startValue) {
		this.startValue = startValue;
		this.policies = policies;
		for (PairPolicyGroup wrapper : policies) {
			BigInteger next = BigInteger.valueOf(wrapper.getPart1() + wrapper.getPart2());
			lcm = (lcm == null) ? next : lcm.multiply(next).divide(lcm.gcd(next));
		}

		currentSize = BigInteger.ZERO;

		for (PairPolicyGroup wrapper : policies) {
			sequencers.add(new PairSequencer(wrapper));
		}

		proceed();
	}

	@Override
	public CloneableTriggerPolicy<IN> clone() {
		return new PairsEnforcerPolicy<IN>(this.policies, startValue);
	}

	@Override
	public double getNextTriggerPosition(double previousTriggerPosition) {
		double ret;
		if (previousTriggerPosition < 0) {
			ret = startValue + getLastMax();
		} else {
			proceed();
			ret = previousTriggerPosition + getLastMax();
		}

		//System.err.println(++tmp2Counter+"^^ " + previousTriggerPosition + " :: " + ret + " :: " + sequence);
		return ret;
	}

	private long proceed() {
		//reset global window when LCM has been reached
		if (currentSize.compareTo(lcm) == 0) {
			currentSize = BigInteger.ZERO;
			for (PairSequencer sequencer : sequencers) {
				sequencer.reset();
			}
		}

		//compute next pair
		List<Long> pairCandidates = new ArrayList<>();
		for (PairSequencer group : sequencers) {
			pairCandidates.add(group.getCurrValue());
		}
		long min = Collections.min(pairCandidates);

		//add next pair in sequence
		sequence.addLast(min);

		currentSize = currentSize.add(BigInteger.valueOf(min));
		for (PairSequencer sequencer : sequencers) {
			sequencer.proceed(min);
		}
		return min;
	}

	@Override
	public boolean notifyTrigger(IN datapoint) {
		//System.err.println(++tmpCounter + " :: " + sequence);
		
		if (counter >= getCurrentMax()) {
			counter = 1;
			lastChunk = getCurrentMax().intValue();
			sequence.removeFirst();
			return true;
		} else {
			counter++;
			return false;
		}
	}

	public List<Long> getSequence() {
		return sequence;
	}

	private Long getCurrentMax() {
		return sequence.getFirst();
	}

	private Long getLastMax() {
		return sequence.getLast();
	}

	@Override
	public double getLowerBorder(double upperBorder) {
		return upperBorder - sequence.getLast();
	}

	@Override
	public int notifyEviction(IN datapoint, boolean triggered, int bufferSize) {
		if (triggered) {
			return lastChunk;
		}
		return 0;
	}
}
