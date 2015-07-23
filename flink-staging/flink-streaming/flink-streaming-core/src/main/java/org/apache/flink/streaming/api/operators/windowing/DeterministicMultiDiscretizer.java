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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.windowbuffer.EagerHeapAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("unused")
public class DeterministicMultiDiscretizer<IN> extends
        AbstractStreamOperator<Tuple2<Integer, IN>> implements
        OneInputStreamOperator<IN, Tuple2<Integer, IN>> {

    private static final Logger LOG = LoggerFactory.getLogger(DeterministicMultiDiscretizer.class);
    private final ReduceFunction<IN> reducer;
    private List<DeterministicPolicyGroup<IN>> policyGroups;
    private HashMap<Integer, LinkedList<Integer>> queryBorders;
    private HashMap<Integer, Integer> partialRefs;
    private WindowAggregator<IN> aggregator;
    private final TypeSerializer<IN> serializer;

    private int partialCnt = 0;
    private final IN identityValue;
    /**
     * The current inter-border aggregate
     */
    private IN currentPartial;


    public DeterministicMultiDiscretizer(
            List<DeterministicPolicyGroup<IN>> policyGroups,
            ReduceFunction<IN> reduceFunction, IN identityValue, int capacity, TypeSerializer<IN> serializer) {

        this.policyGroups = policyGroups;
        this.serializer = serializer;
        /**
         * A mapping of border IDs per query where border ID is the partial aggregate ID to start an aggregation 
         */
        this.queryBorders = new HashMap<Integer, LinkedList<Integer>>();
        /**
         * Partial aggregate reference counter for garbage collection
         */
        this.partialRefs = new HashMap<Integer, Integer>();
        /**
         * An aggregator for pre-computing all shared preaggregates per partial result addition
         */
        this.aggregator = new EagerHeapAggregator<IN>(reduceFunction, serializer, identityValue, capacity);
        this.reducer = reduceFunction;

        for (int i = 0; i < this.policyGroups.size(); i++) {
            queryBorders.put(i, new LinkedList<Integer>());
        }

        this.identityValue = identityValue;
        this.currentPartial = identityValue;

        chainingStrategy = ChainingStrategy.ALWAYS;
    }


    @SuppressWarnings("unchecked")
    @Override
    public void processElement(IN tuple) throws Exception {
        // First handle the deterministic policies
        LOG.info("Processing element " + tuple);
        for (int i = 0; i < policyGroups.size(); i++) {
            int windowEvents = policyGroups.get(i).getWindowEvents(tuple);

            if (windowEvents != 0) {
                
                // **STRATEGY FOR REGISTERING PARTIALS**
                // 1) first partial does not need to be added in the pre-aggregation buffer
                // 2) we only need to add eviction borders in the pre-aggregation buffer - consecutive triggers
                //    can reuse the current partial on-the-fly (no need to pre-aggregate that)!
                
                if((windowEvents >> 16) > 0) {
                    if (partialCnt != 0){
                        aggregator.add(partialCnt, currentPartial);
                    }
                    partialCnt++;
                    currentPartial = identityValue;
                }

                for (int j = 0; j < (windowEvents >> 16); j++) {
                    queryBorders.get(i).add(partialCnt);
                    registerPartial();
                }
                for (int j = 0; j < (windowEvents & 0xFFFF); j++) {
                    collectAggregate(i);
                }
            }
        }
        currentPartial = reducer.reduce(serializer.copy(currentPartial), tuple);
    }

    private void registerPartial() {
        if (!partialRefs.containsKey(partialCnt)) {
            partialRefs.put(partialCnt, 1);
        } else {
            partialRefs.put(partialCnt, partialRefs.get(partialCnt) + 1);
        }
    }

    private void unregisterPartial(int partialId) throws Exception {
        int next = partialRefs.get(partialId) - 1;
        if (next == 0) {
            LOG.info("REMOVING PARTIAL {}",partialId);
            partialRefs.remove(partialId);
            aggregator.remove(partialId);
        } else {
            partialRefs.put(partialId, next);
        }
    }

    private void collectAggregate(int queryId) throws Exception {
        Integer partial = queryBorders.get(queryId).getFirst();
        LOG.info("Q{} Emitting window from partial id: {}", queryId, partial);
        output.collect(new Tuple2<Integer, IN>(queryId, reducer.reduce(serializer.copy(aggregator.aggregate(partial)), 
                serializer.copy(currentPartial))));
        queryBorders.get(queryId).removeFirst();
        unregisterPartial(partial);
    }

}
