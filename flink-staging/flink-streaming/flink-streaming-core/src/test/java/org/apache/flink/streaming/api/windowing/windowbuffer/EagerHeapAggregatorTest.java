/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.streaming.api.windowing.windowbuffer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class EagerHeapAggregatorTest {

    private EagerHeapAggregator<Integer> aggr;

    @Before
    public void setUp() throws Exception {
        aggr = new EagerHeapAggregator<Integer>(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        }, IntSerializer.INSTANCE, 0, 2);
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