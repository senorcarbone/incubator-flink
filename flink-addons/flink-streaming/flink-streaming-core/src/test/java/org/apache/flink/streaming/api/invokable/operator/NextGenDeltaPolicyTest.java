package org.apache.flink.streaming.api.invokable.operator;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class NextGenDeltaPolicyTest {

	@Test
	public void testDelta() {
		NextGenDeltaPolicy deltaPolicy = new NextGenDeltaPolicy(new NextGenDeltaFunction<Tuple2<Integer, Integer>>() {
			@Override
			public double getDelta(Tuple2<Integer, Integer> oldDataPoint, Tuple2<Integer, Integer> newDataPoint) {
				return (double) newDataPoint.f0 - oldDataPoint.f0;
			}
		}, new Tuple2(0, 0), 2);

		ArrayList<Tuple2> tuples = Lists.newArrayList(
				new Tuple2(1, 0),
				new Tuple2(2, 0),
				new Tuple2(3, 0),
				new Tuple2(6, 0));

		assertFalse(deltaPolicy.notifyTrigger(tuples.get(0)));
		assertEquals(0, deltaPolicy.notifyEviction(tuples.get(0), false, 0));

		assertFalse(deltaPolicy.notifyTrigger(tuples.get(1)));
		assertEquals(0, deltaPolicy.notifyEviction(tuples.get(1), false, 1));

		assertTrue(deltaPolicy.notifyTrigger(tuples.get(2)));
		assertEquals(1, deltaPolicy.notifyEviction(tuples.get(2), true, 2));

		assertTrue(deltaPolicy.notifyTrigger(tuples.get(3)));
		assertEquals(2, deltaPolicy.notifyEviction(tuples.get(3), true, 2));
	}


}