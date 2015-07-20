package org.apache.flink.streaming.api.windowing.windowbuffer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class FatAggregatorTest {

    private FatAggregator<Integer> aggr;

    @Before
    public void setUp() throws Exception {
        aggr = new FatAggregator<Integer>(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        }, 0, 8);
    }


    @Test
    public void testAggregator() throws Exception {
        
        //testing typical circular heap operations
        aggr.add(1, 1);
        assertEquals(Integer.valueOf(1), aggr.aggregate());
        aggr.add(2, 2);
        aggr.add(3, 3);
        assertEquals(Integer.valueOf(6), aggr.aggregate());
        aggr.add(4, 4);
        aggr.add(5, 5);
        aggr.add(6, 6);
        aggr.add(7, 7);
        assertEquals(Integer.valueOf(28), aggr.aggregate());
        aggr.remove(1);
        assertEquals(Integer.valueOf(27), aggr.aggregate());
        aggr.add(8, 8);
        assertEquals(Integer.valueOf(35), aggr.aggregate());
        assertEquals(Integer.valueOf(30), aggr.aggregate(4));

        // testing reverse circular heap operations
        aggr.add(9, 9);
        assertEquals(Integer.valueOf(44), aggr.aggregate());
        assertEquals(Integer.valueOf(42), aggr.aggregate(3));
        aggr.remove(2);
        aggr.add(10, 10);
        assertEquals(Integer.valueOf(52), aggr.aggregate());
        assertEquals(Integer.valueOf(49), aggr.aggregate(4));
    }

}