package org.apache.flink.streaming.api.operators.windowing;

import junit.framework.TestCase;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.PairsEnforcerPolicy;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PairDiscretizationTest extends TestCase {


	@Test
	public void testPairs() {
		List<PairPolicyGroup<Integer>> policyGroups = new ArrayList<>();
		policyGroups.add(new PairPolicyGroup<>(
				new DeterministicPolicyGroup<>(
						new DeterministicCountTriggerPolicy<>(15),
						new DeterministicCountEvictionPolicy<>(18))));
		policyGroups.add(new PairPolicyGroup<>(
				new DeterministicPolicyGroup<>(
						new DeterministicCountTriggerPolicy<>(9),
						new DeterministicCountEvictionPolicy<>(12))));

		DeterministicPolicyGroup group = PairDiscretization.getCommonPairPolicy(policyGroups);
		PairsEnforcerPolicy trigger = (PairsEnforcerPolicy) group.getTrigger();


//		double lastPos = -1;
//		for (int i = 0; i < 24; i++) {
//			System.err.println(trigger.getLowerBorder(trigger.getNextTriggerPosition(lastPos)));
//		}

//		assertEquals(Lists.newArrayList(6l, 3l, 3l, 3l, 3l, 6l, 3l, 3l, 3l, 3l, 6l, 3l, 6l, 3l, 3l, 3l, 3l, 6l, 3l, 3l, 3l, 3l, 6l, 3l), trigger.getSequence());
//
//		for (int i = 0; i < 24; i++) {
//			System.err.println(trigger.notifyTrigger(i));
//		}
	}

}