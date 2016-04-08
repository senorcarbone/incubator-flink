package org.apache.flink.streaming.paper.experiments;

import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingSensorPolicyGroup;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SensorPunctuationWindowDetTest extends TestCase {

	/*********************************************
	 * Data                                      *
	 *********************************************/

	List<Tuple2<Integer, Integer>> inputs1 = new ArrayList<>();

	{
		inputs1.add(new Tuple2<>(1, 1));

	}

	List<String> scenario = Lists.newArrayList(
			"00100000",
			"00100000",
			"00000100",
			"00000100",
			"00100100",
			"00100000"
	);

	List<Tuple2<Integer, Integer>> expected = Lists.newArrayList(
			new Tuple2<>(2, 0),  // 5 start 2 start
			new Tuple2<>(0, 0), // --
			new Tuple2<>(2, 2), // 5 restart 2 restart
			new Tuple2<>(0, 0),
			new Tuple2<>(1, 1), // 5 restarts
			new Tuple2<>(1, 1) // 2 restarts
	);

	/*********************************************
	 * Tests                                     *
	 *********************************************/

	@Test
	public void testMultiDiscretizerDeterministic() {

		//prepare policies
		DeterministicTriggerPolicy<Tuple4<Long, Long, Long, Integer>> policy = new SensorPunctuationWindowDet(5);
		TumblingSensorPolicyGroup<Tuple4<Long, Long, Long, Integer>> pgroup = new TumblingSensorPolicyGroup<>(policy);

		DeterministicTriggerPolicy<Tuple4<Long, Long, Long, Integer>> policy2 = new SensorPunctuationWindowDet(2);
		TumblingSensorPolicyGroup<Tuple4<Long, Long, Long, Integer>> pgroup2 = new TumblingSensorPolicyGroup<>(policy2);

		assertEquals(expected, sumEvents(getTumblingEvents(scenario, pgroup), getTumblingEvents(scenario, pgroup2)));
	}


	private static List<Tuple2<Integer, Integer>> getTumblingEvents(List<String> binarray,
																	DeterministicPolicyGroup<Tuple4<Long, Long, Long, Integer>> pGroup) {
		List<Tuple2<Integer, Integer>> results = new ArrayList<>();
		for (String evt : binarray) {
			results.add(convertEvent(pGroup.getWindowEvents(getEvent(evt))));
		}
		return results;
	}

	/**
	 * Converts event to Tuple2<Boolean, Boolean> : (started, ended)
	 *
	 * @param windowEvents
	 * @return
	 */
	private static Tuple2<Integer, Integer> convertEvent(int windowEvents) {
		return new Tuple2<>(windowEvents >> 16, windowEvents & 0xFFFF);
	}

	private static Tuple4<Long, Long, Long, Integer> getEvent(String binaryStr) {
		return new Tuple4<>(0l, 0l, 0l, Integer.parseInt(binaryStr, 2));
	}
	
	private static List<Tuple2<Integer, Integer>> sumEvents(List<Tuple2<Integer, Integer>>... events){
		int size = events[0].size();
		
		List<Tuple2<Integer, Integer>> summed = new ArrayList<>(size);
		
		for(int i = 0; i<size; i++){
			Tuple2<Integer, Integer> next = new Tuple2<>(0,0);
			for(List<Tuple2<Integer, Integer>> res : events){
				next = new Tuple2<>(res.get(i).f0 + next.f0, res.get(i).f1 + next.f1);
			}
			summed.add(next);
		}
		
		return summed;		
	}

}