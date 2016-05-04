package org.apache.flink.streaming.api.operators.windowing;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.PairsEnforcerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.TempPolicyGroup;
import org.apache.flink.streaming.paper.AggregationFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class PairDiscretization {

	private static final Logger LOG = LoggerFactory.getLogger(PairDiscretization.class);

	/**
	 * It creates a new MultiDiscretizer that emulates the logic of operator sharing using pairs, for the given policies
	 *
	 * @param policyGroups
	 * @param reduceFunction
	 * @param identityValue
	 * @param capacity
	 * @param serializer
	 * @param aggregationType
	 * @param <IN>
	 * @return
	 */
	public static <IN, AGG extends Serializable> Cutty<IN, AGG> create(List<DeterministicPolicyGroup<IN>> policyGroups, ReduceFunction<AGG> reduceFunction,
																	   AGG identityValue, int capacity, TypeSerializer<AGG> serializer,
																	   AggregationFramework.AGGREGATION_STRATEGY aggregationType) {
		ensureCompatibility(policyGroups);

		LinkedList<DeterministicPolicyGroup<IN>> groups = new LinkedList<>();

		for (DeterministicPolicyGroup group : policyGroups) {
			groups.add(new PairPolicyGroup<>(group));
		}
		groups.addFirst(getCommonPairPolicy((List<PairPolicyGroup<IN>>) (List) groups));

		return new Cutty<>(groups, reduceFunction, identityValue, capacity, serializer, aggregationType);

	}

	/**
	 * Performs a check to see if all policies are compatible (i.e. of the same measure such as count or event time)
	 *
	 * @param policyGroups
	 */
	private static <IN> void ensureCompatibility(List<DeterministicPolicyGroup<IN>> policyGroups) throws IllegalArgumentException {
		Class policyClass = null;
		for (DeterministicPolicyGroup<IN> group : policyGroups) {
			if (policyClass == null) {
				policyClass = group.getTrigger().getClass();
			} else {
				if (!policyClass.equals(group.getTrigger().getClass())) {
					throw new IllegalArgumentException("Pairing only works with policies of the same measure. \n" +
							"Mismatch of policies detected :: " + policyClass + " and " + group.getTrigger().getClass());
				}
			}
		}
	}

	/**
	 * Computes the two pre-aggregation panes, knows as 'pairs aggregation technique' defined by Krishnamurthy et.al.
	 *
	 * @param range
	 * @param slide
	 * @return
	 */
	public static long[] computePairs(long range, long slide) {
		long[] pairs = new long[2];
		pairs[0] = range % slide;
		pairs[1] = slide - pairs[0];
		return pairs;
	}

	/**
	 * The generated policy contains the extended window with all panes shared by all given policies. The size of the
	 * window is equal to the least common multiple of the period (part1+part2) among the given policies. This policy can
	 * be used in the same DeterministicMultiDiscretizer with the given policies to emulate the execution of pairs.
	 *
	 * @param policies
	 * @param <DATA>
	 * @return
	 */
	public static <DATA> DeterministicPolicyGroup<DATA> getCommonPairPolicy(List<PairPolicyGroup<DATA>> policies) {

		PairPolicyGroup group = policies.get(0);
		
		int offset = ((DeterministicCountTriggerPolicy) group.getTrigger()).getSlide()
				- ((DeterministicCountEvictionPolicy) group.getEviction()).getRange(); 
		
		if (group.getEviction() instanceof DeterministicCountEvictionPolicy) {
			PairsEnforcerPolicy<DATA> pairsEnforcer = new PairsEnforcerPolicy<>(policies,
					((DeterministicCountTriggerPolicy) group.getTrigger()).getStartValue() + offset);
			
			return new TempPolicyGroup<>(pairsEnforcer, pairsEnforcer, group.getFieldExtractor());
		} else
			throw new IllegalArgumentException("Only count based policies are currently supported for pairs ");
		
	}
}
