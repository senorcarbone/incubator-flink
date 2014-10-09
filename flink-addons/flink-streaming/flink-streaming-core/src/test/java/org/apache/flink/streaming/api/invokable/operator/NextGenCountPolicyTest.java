package org.apache.flink.streaming.api.invokable.operator;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;


public class NextGenCountPolicyTest {


	@Test
	public void testDelta() {

		NextGenTriggerPolicy triggerPolicy = Count.of(2).toTrigger();
		NextGenEvictionPolicy evictPolicy = Count.of(5).toEvict();

		ArrayList tuples = Lists.newArrayList(1,2,3,4,5,6,7);

		assertFalse(triggerPolicy.notifyTrigger(tuples.get(0)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(0), false, 0));

		assertFalse(triggerPolicy.notifyTrigger(tuples.get(1)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(1), false, 1));

		assertTrue(triggerPolicy.notifyTrigger(tuples.get(2)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(2), true, 2));

		assertFalse(triggerPolicy.notifyTrigger(tuples.get(3)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(3), true, 3));

		assertTrue(triggerPolicy.notifyTrigger(tuples.get(4)));
		assertEquals(0, evictPolicy.notifyEviction(tuples.get(4), true, 4));

		assertFalse(triggerPolicy.notifyTrigger(tuples.get(5)));
		assertEquals(1, evictPolicy.notifyEviction(tuples.get(5), true, 5));

		assertTrue(triggerPolicy.notifyTrigger(tuples.get(6)));
		assertEquals(1, evictPolicy.notifyEviction(tuples.get(6), true, 5));
	}

}
