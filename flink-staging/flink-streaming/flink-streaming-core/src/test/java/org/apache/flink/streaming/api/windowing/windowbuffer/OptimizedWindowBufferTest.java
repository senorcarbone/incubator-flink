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
import org.apache.flink.api.java.tuple.Tuple1;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OptimizedWindowBufferTest {

    @Test
    public void testDeterministic() throws Exception {

        OptimizedWindowBuffer<Tuple1<String>> optimizedWindowBuffer = new OptimizedWindowBuffer<Tuple1<String>>(new TestReduceFunction(),true,false);

        //Try single window
        int window1 = optimizedWindowBuffer.startWindow();
        optimizedWindowBuffer.store(new Tuple1<String>("1"));
        optimizedWindowBuffer.store(new Tuple1<String>("2"));
        optimizedWindowBuffer.store(new Tuple1<String>("3"));
        Tuple1<String> window1Result = optimizedWindowBuffer.endWindow(window1);
        compareStrings("1|2|3", window1Result.f0);

        //Try multiple overlapping windows
        optimizedWindowBuffer.store(new Tuple1<String>("1"));
        optimizedWindowBuffer.store(new Tuple1<String>("2"));
        int window2 = optimizedWindowBuffer.startWindow();
        int window3 = optimizedWindowBuffer.startWindow();
        optimizedWindowBuffer.store(new Tuple1<String>("3"));
        optimizedWindowBuffer.store(new Tuple1<String>("4"));
        Tuple1<String> window3Result = optimizedWindowBuffer.endWindow(window3);
        compareStrings("3|4", window3Result.f0);
        optimizedWindowBuffer.store(new Tuple1<String>("5"));
        int window4 = optimizedWindowBuffer.startWindow();
        optimizedWindowBuffer.store(new Tuple1<String>("6"));
        Tuple1<String> window2Result = optimizedWindowBuffer.endWindow(window2);
        compareStrings("3|4|5|6", window2Result.f0);
        optimizedWindowBuffer.store(new Tuple1<String>("7"));
        optimizedWindowBuffer.store(new Tuple1<String>("8"));
        optimizedWindowBuffer.store(new Tuple1<String>("9"));
        Tuple1<String> window4Result = optimizedWindowBuffer.endWindow(window4);
        compareStrings("6|7|8|9", window4Result.f0);

        //Try empty window
        int window5 = optimizedWindowBuffer.startWindow();
        Tuple1<String> window5Result = optimizedWindowBuffer.endWindow(window5);
        assertNull(window5Result);
    }

    @Test
    public void testNotDeterministic() throws Exception{

        OptimizedWindowBuffer<Tuple1<String>> optimizedWindowBuffer = new OptimizedWindowBuffer<Tuple1<String>>(new TestReduceFunction(),false,true);

        //Try single window
        int query1 = optimizedWindowBuffer.registerQuery();
        optimizedWindowBuffer.store(new Tuple1<String>("1"));
        optimizedWindowBuffer.store(new Tuple1<String>("2"));
        optimizedWindowBuffer.store(new Tuple1<String>("3"));
        Tuple1<String> window1Result = optimizedWindowBuffer.emitWindow(query1);
        compareStrings("1|2|3", window1Result.f0);

        //Try multiple overlapping windows
        optimizedWindowBuffer.evict(3,query1);
        optimizedWindowBuffer.store(new Tuple1<String>("1"));
        optimizedWindowBuffer.store(new Tuple1<String>("2"));
        optimizedWindowBuffer.evict(2,query1);
        int query2 = optimizedWindowBuffer.registerQuery();
        optimizedWindowBuffer.store(new Tuple1<String>("3"));
        optimizedWindowBuffer.store(new Tuple1<String>("4"));
        Tuple1<String> window3Result = optimizedWindowBuffer.emitWindow(query2);
        compareStrings("3|4", window3Result.f0);
        optimizedWindowBuffer.store(new Tuple1<String>("5"));
        optimizedWindowBuffer.store(new Tuple1<String>("6"));
        Tuple1<String> window2Result = optimizedWindowBuffer.emitWindow(query1);
        compareStrings("3|4|5|6", window2Result.f0);
        optimizedWindowBuffer.store(new Tuple1<String>("7"));
        optimizedWindowBuffer.store(new Tuple1<String>("8"));
        optimizedWindowBuffer.store(new Tuple1<String>("9"));
        optimizedWindowBuffer.evict(3,query1);
        Tuple1<String> window4Result = optimizedWindowBuffer.emitWindow(query1);
        compareStrings("6|7|8|9", window4Result.f0);

        //Try empty window
        int window5 = optimizedWindowBuffer.startWindow();
        Tuple1<String> window5Result = optimizedWindowBuffer.endWindow(window5);
        assertNull(window5Result);

    }

    @Test
    public void testMixed() throws Exception{

        OptimizedWindowBuffer<Tuple1<String>> optimizedWindowBuffer = new OptimizedWindowBuffer<Tuple1<String>>(new TestReduceFunction(),true,true);

        //Try single window
        int window1 = optimizedWindowBuffer.startWindow();
        int query1 = optimizedWindowBuffer.registerQuery();
        optimizedWindowBuffer.store(new Tuple1<String>("1"));
        optimizedWindowBuffer.store(new Tuple1<String>("2"));
        optimizedWindowBuffer.store(new Tuple1<String>("3"));
        Tuple1<String> window1Result = optimizedWindowBuffer.endWindow(window1);
        compareStrings("1|2|3", window1Result.f0);
        Tuple1<String> window6Result = optimizedWindowBuffer.emitWindow(query1);
        compareStrings("1|2|3", window6Result.f0);

        //Try multiple overlapping windows
        optimizedWindowBuffer.evict(3,query1);
        optimizedWindowBuffer.store(new Tuple1<String>("1"));
        optimizedWindowBuffer.store(new Tuple1<String>("2"));
        optimizedWindowBuffer.evict(2,query1);
        int window2 = optimizedWindowBuffer.startWindow();
        int window3 = optimizedWindowBuffer.startWindow();
        int query2 = optimizedWindowBuffer.registerQuery();
        optimizedWindowBuffer.store(new Tuple1<String>("3"));
        optimizedWindowBuffer.store(new Tuple1<String>("4"));
        Tuple1<String> window7Result = optimizedWindowBuffer.emitWindow(query2);
        compareStrings("3|4", window7Result.f0);
        Tuple1<String> window3Result = optimizedWindowBuffer.endWindow(window3);
        compareStrings("3|4", window3Result.f0);
        optimizedWindowBuffer.store(new Tuple1<String>("5"));
        int window4 = optimizedWindowBuffer.startWindow();
        optimizedWindowBuffer.store(new Tuple1<String>("6"));
        Tuple1<String> window2Result = optimizedWindowBuffer.endWindow(window2);
        compareStrings("3|4|5|6", window2Result.f0);
        Tuple1<String> window8Result = optimizedWindowBuffer.emitWindow(query1);
        compareStrings("3|4|5|6", window8Result.f0);
        optimizedWindowBuffer.store(new Tuple1<String>("7"));
        optimizedWindowBuffer.store(new Tuple1<String>("8"));
        optimizedWindowBuffer.store(new Tuple1<String>("9"));
        Tuple1<String> window4Result = optimizedWindowBuffer.endWindow(window4);
        compareStrings("6|7|8|9", window4Result.f0);
        optimizedWindowBuffer.evict(3,query1);
        Tuple1<String> window9Result = optimizedWindowBuffer.emitWindow(query1);
        compareStrings("6|7|8|9", window9Result.f0);

        //Try empty window
        int window5 = optimizedWindowBuffer.startWindow();
        Tuple1<String> window5Result = optimizedWindowBuffer.endWindow(window5);
        assertNull(window5Result);
    }

    /**
     * This reduce function concatenates the inputs it receives.
     * Inputs are seperated by pipes.
     */
    private class TestReduceFunction implements ReduceFunction<Tuple1<String>>{

        @Override
        public Tuple1<String> reduce(Tuple1<String> value1, Tuple1<String> value2) throws Exception {
            return new Tuple1<String>(value1.f0+"|"+value2.f0);
        }
    }

    private void compareStrings(String expected, String actual){
        assertEquals("Wrong output: expected: "+expected+" actual: "+actual,expected,actual);
    }
}
