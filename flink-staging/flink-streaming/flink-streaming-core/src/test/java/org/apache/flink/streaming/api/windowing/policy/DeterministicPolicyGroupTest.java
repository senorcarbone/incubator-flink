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

package org.apache.flink.streaming.api.windowing.policy;

import org.junit.Test;

import static org.junit.Assert.*;

import java.util.LinkedList;

public class DeterministicPolicyGroupTest {

    //Set window begin and end positions
    private static double[] windowBegins={0d,1d,3d,5d,5d,10d,98d};
    private static double[] windowEnds={5d,7d,7d,8d,10d,15d,99d};
    private static double[] windowBegins2={0d,1d,3d,5d,10d,98d};
    private static double[] windowEnds2={5d,7d,8d,10d,15d,99d};

    //Set the numbers of opened and closed windows for each seq. number
    //                                           0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    private static short[] windowBeginCounters= {1,1,0,1,0,2,0,0,0,0, 1, 0, 0, 0, 0, 0};
    private static short[] windowEndCounters =  {0,0,0,0,0,1,0,2,1,0, 1, 0, 0, 0, 0, 1};
    private static short[] windowBeginCounters2={1,1,0,1,0,1,0,0,0,0, 1, 0, 0, 0, 0, 0};
    private static short[] windowEndCounters2 = {0,0,0,0,0,1,0,1,1,0, 1, 0, 0, 0, 0, 1};

    /**
     * This test assumes the following windows:
     * 0-5,1-7,3-7,5-8,5-10,10-15
     *
     * Window begins are at: 0,1,3,5,5,10
     * Window ends are at: 5,7,7,8,10,15
     */
    @Test
    public void runTest(){

        LinkedList<Integer> results=new LinkedList<Integer>();

        DeterministicSequenceTrigger<Double> trigger=new DeterministicSequenceTrigger<Double>();
        DeterministicSequenceEvictor<Double> evictor=new DeterministicSequenceEvictor<Double>();
        DeterministicPolicyGroup<Double> group = new DeterministicPolicyGroup<Double>(trigger,evictor);

        for (double i=0;i<=15; i++){
            results.add(group.getWindowEvents(i));
        }

        //Check results for window begins and ends
        for (int i=0;i<=15; i++){
            assertEquals("The window begin counter at position "+i+" was wrong.",windowBeginCounters[i],results.get(i)>>16);
            assertEquals("The window end counter at position "+i+" was wrong.",windowEndCounters[i],results.get(i)&0xFFFF);
        }

        //check that regular notification was done
        for (double i=0;i<=15; i++){
            assertTrue("Missing notification of the trigger (position "+i+")",trigger.getDataItems().contains(i));
            assertTrue("Missing notification of the evictor (position "+i+")",evictor.getDataItems().contains(i));
        }
    }

    /**
     * This is the same test as the one before, except that a recomputation of the lookahead is done
     * due to the annotation of the used trigger policy
     */
    @Test
    public void runTestWithRecompute(){

        LinkedList<Integer> results=new LinkedList<Integer>();

        RecomputeLookaheadSequenceTrigger<Double> trigger=new RecomputeLookaheadSequenceTrigger<Double>();
        RecomputeLookaheadSequenceEvictor<Double> evictor=new RecomputeLookaheadSequenceEvictor<Double>();
        DeterministicPolicyGroup<Double> group = new DeterministicPolicyGroup<Double>(trigger,evictor);

        for (double i=0;i<=15; i++){
            results.add(group.getWindowEvents(i));
        }

        //Check results for window begins and ends
        for (int i=0;i<=15; i++){
            assertEquals("The window begin counter at position "+i+" was wrong.",windowBeginCounters2[i],results.get(i)>>16);
            assertEquals("The window end counter at position "+i+" was wrong.",windowEndCounters2[i],results.get(i)&0xFFFF);
        }

        //check that regular notification was done
        for (double i=0;i<=15; i++){
            assertTrue("Missing notification of the trigger (position "+i+")",trigger.getDataItems().contains(i));
            assertTrue("Missing notification of the evictor (position "+i+")",evictor.getDataItems().contains(i));
        }

        //Check that lookahead was recomputed
        assertEquals("The number of calls to the trigger policy seem to not include sufficient lookahead recomputation",47,trigger.callCounter);
        assertEquals("The number of calls to the eviction policy seem to not include sufficient lookahead recomputation",47,evictor.callCounter);
    }

    /**
     * A simple deterministic trigger policy, only for testing.
     * @param <DATA> The type of input data handled by this policy
     */
    private class DeterministicSequenceTrigger<DATA> implements DeterministicTriggerPolicy<DATA>{

        int counter=0;
        LinkedList<DATA> dataItems=new LinkedList<DATA>();

        @Override
        public double getNextTriggerPosition(double previouseTriggerPosition) {
            if (windowEnds[counter]>=previouseTriggerPosition){
                return windowEnds[counter++];
            }
            throw new RuntimeException("Unexpected value of previouseTriggerPosition.");
        }

        @Override
        public boolean notifyTrigger(DATA datapoint) {
            this.dataItems.add(datapoint);
            //Return value is not evaluated.
            return false;
        }

        public LinkedList<DATA> getDataItems(){
            return dataItems;
        }
    }

    /**
     * A simple deterministic eviction policy, only for testing.
     * @param <DATA> The type of input data handled by this policy
     */
    private class DeterministicSequenceEvictor<DATA> implements DeterministicEvictionPolicy<DATA>{

        int counter=0;
        LinkedList<DATA> dataItems=new LinkedList<DATA>();

        @Override
        public double getLowerBorder(double upperBorder) {
            if (windowEnds[counter]==upperBorder){
                return windowBegins[counter++];
            } else {
                throw new RuntimeException("Wrong upper border: "+upperBorder);
            }
        }

        @Override
        public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize) {
            this.dataItems.add(datapoint);
            //Return value is not evaluated.
            return 0;
        }

        public LinkedList<DATA> getDataItems(){
            return dataItems;
        }
    }

    /**
     * A simple deterministic trigger policy with Annotation, only for testing.
     * @param <DATA> The type of input data handled by this policy
     */
    @RecomputeLookahead
    private class RecomputeLookaheadSequenceTrigger<DATA> extends DeterministicSequenceTrigger{

        int callCounter=0;

        @Override
        public double getNextTriggerPosition(double previouseTriggerPosition) {
            callCounter++;
            if (previouseTriggerPosition<0d) return windowEnds2[0];
            int i=1;
            while (windowEnds2[i]<=previouseTriggerPosition){
                i++;
            }
            return windowEnds2[i];
        }

    }

    /**
     * A simple deterministic eviction policy, only for testing.
     * @param <DATA> The type of input data handled by this policy
     */
    private class RecomputeLookaheadSequenceEvictor<DATA> extends DeterministicSequenceEvictor{

        int callCounter=0;

        @Override
        public double getLowerBorder(double upperBorder) {
            callCounter++;
            int i=0;
            while (windowEnds2[i]!=upperBorder){
                i++;
            }
            return windowBegins2[i];
        }
    }

}
