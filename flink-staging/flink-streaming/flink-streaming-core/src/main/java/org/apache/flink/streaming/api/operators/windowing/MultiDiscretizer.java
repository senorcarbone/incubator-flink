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

package org.apache.flink.streaming.api.operators.windowing;

import org.apache.flink.streaming.api.windowing.policy.*;

import java.util.LinkedList;

/**
 * This class represents a merged discretization operator handling discretizations from several queries.
 *
 * NOTICE: This is a untested version of this class (Implementation still in progress)
 *
 * @param <IN> The type of input tuples handled by this operator
 */
@SuppressWarnings("unused")
public class MultiDiscretizer<IN> {

    private LinkedList<DeterministicPolicyGroup<IN>> deterministicPolicyGroups;
    LinkedList<TriggerPolicy<IN>> triggerPolicies;
    LinkedList<EvictionPolicy<IN>> evictionPolicies;
    int[] bufferSizes = new int[triggerPolicies.size()];
    boolean[] isActiveTrigger = new boolean[triggerPolicies.size()];
    boolean[] isActiveEviction = new boolean[evictionPolicies.size()];

    /**
     * This constructor takes the policies of multiple queries.
     * @param deterministicPolicyGroups The policy groups representing the deterministic queries
     * @param notDeterministicTriggerPolicies The trigger policies of the not deterministic queries.
     *                                        This must be the same size as the eviction policy list!
     * @param notDeterministicEvictionPolicies The eviction policies of the not deterministic queries.
     *                                         This must have the same size as the trigger policy list!
     */
    public MultiDiscretizer(LinkedList<DeterministicPolicyGroup<IN>> deterministicPolicyGroups,
                            LinkedList<TriggerPolicy<IN>> notDeterministicTriggerPolicies,
                            LinkedList<EvictionPolicy<IN>> notDeterministicEvictionPolicies){
        this.deterministicPolicyGroups=deterministicPolicyGroups;
        this.evictionPolicies = notDeterministicEvictionPolicies;
        this.triggerPolicies = notDeterministicTriggerPolicies;

        //Catch active policies
        for (int i=0; i<isActiveEviction.length; i++){

            //Active trigger
            isActiveTrigger[i] = triggerPolicies.get(i) instanceof ActiveTriggerPolicy;

            //Active eviction
            isActiveEviction[i] = evictionPolicies.get(i) instanceof ActiveEvictionPolicy;

            bufferSizes[i]=0;
        }

    }

    @SuppressWarnings("unchecked")
    private void processInputTuple(IN tuple){
        //First handle the deterministic policies
        for (int i=0; i<deterministicPolicyGroups.size(); i++){
            int windowEvents=deterministicPolicyGroups.get(i).getWindowEvents(tuple);
            //processWindowBegins
            for (int j=0; j<(windowEvents>>16); j++){
                beginWindow(i);
            }
            //processWindowEnds
            for (int j=0; j<(windowEvents&0xFFFF); j++){
                endWindow(i);
            }
        }

        //Now handle the not deterministic queries
        for (int i=0; i< triggerPolicies.size(); i++){

            //Do pre-notification for active triggers
            if (isActiveTrigger[i]){
                Object[] preNotificationTuples = ((ActiveTriggerPolicy)triggerPolicies.get(i)).preNotifyTrigger(tuple);
                for (Object preNotificationTuple:preNotificationTuples){
                    if (isActiveEviction[i]){
                        evict(i,((ActiveEvictionPolicy)evictionPolicies.get(i)).notifyEvictionWithFakeElement(preNotificationTuple,bufferSizes[i]));
                    }
                    emitWindow(i);
                }
            }

            //Do regular notification
            if (triggerPolicies.get(i).notifyTrigger(tuple)){
                emitWindow(i);
                evict(i,evictionPolicies.get(i).notifyEviction(tuple, true, bufferSizes[i]));
            } else {
                evict(i,evictionPolicies.get(i).notifyEviction(tuple, false, bufferSizes[i]));
            }

        }

        //Finally add the current tuple to the buffer
        store(tuple);
    }


    /**********************************************************************************************

    The following method can be used to send event to the aggregation operation (B2B Buffer class)
    Alternatively, the aggregation could be done in here, having an instance of B2BBuffer.

     ********************************************************************************************/

    /**
     * Send a window begin marker for a deterministic policy group
     * @param queryId the query this marker belongs to.
     *                Remark; deterministic and not deterministic policies are numbered separately!
     */
    private void beginWindow(int queryId){
        //TODO
    }

    /**
     * Send a window end marker for a deterministic policy group
     * @param queryId the query this marker belongs to
     *                Remark; deterministic and not deterministic policies are numbered separately!
     */
    private void endWindow(int queryId){
        //TODO
    }

    /**
     * Adds the given tuple to the buffer
     * @param tuple the input tuple
     */
    private void store(IN tuple){
        for (int i=0;i<bufferSizes.length;i++){
            bufferSizes[i]=bufferSizes[i]+1;
        }
        //TODO
    }

    /**
     * Sends a window end even for not deterministic policies.
     * @param queryId the query this marker belongs to
     *                Remark; deterministic and not deterministic policies are numbered separately!
     */
    private void emitWindow(int queryId){
        //TODO
    }

    /**
     * Sends a eviction request for a not deterministic query
     * @param query the query this marker belongs to
     *              Remark; deterministic and not deterministic policies are numbered separately!
     * @param n the number of tuple to delete from the buffer
     */
    private void evict(int query, int n){
        if (n>0){
            bufferSizes[query]=bufferSizes[query]-n;
            //TODO
        }
    }

}
