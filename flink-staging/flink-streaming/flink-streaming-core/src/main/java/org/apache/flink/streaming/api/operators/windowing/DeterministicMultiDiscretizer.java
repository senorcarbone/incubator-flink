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

import java.util.*;

@SuppressWarnings("unused")
public class DeterministicMultiDiscretizer<IN> extends
        AbstractStreamOperator<Tuple2<Integer, IN>> implements
        OneInputStreamOperator<IN, Tuple2<Integer, IN>> {

    private static final Logger LOG = LoggerFactory.getLogger(DeterministicMultiDiscretizer.class);

    /**
     * A user given reduce function used for continuously combining pre-aggregates
     */
    private final ReduceFunction<IN> reducer;
    /**
     * The identity value for the given combine function where reduce(val, identity) == reduce(identity, val) == val 
     */
    private final IN identityValue;
    /**
     * All the policy groups that are co-located in one multi-discretizer
     */
    private List<DeterministicPolicyGroup<IN>> policyGroups;
    /**
     * A mapping of border IDs per query where border ID is the partial aggregate ID to start an aggregation
     */
    private Map<Integer, Deque<Integer>> queryBorders;
    /**
     * Partial aggregate reference counter for garbage collection
     */
    private Map<Integer, Integer> partialDependencies;
    /**
     * The next partial id in the queue for garbage collection
     */
    private int partialGC = 1;
    /**
     * An aggregator for pre-computing all shared preaggregates per partial result addition
     */
    private WindowAggregator<IN> aggregator;
    /**
     * A serializer used for copying values for immutability
     */
    private final TypeSerializer<IN> serializer;

    /**
     * The partial ID index counter
     */
    private int partialIdx = 0;
    /**
     * The current inter-border aggregate
     */
    private IN currentPartial;


    public DeterministicMultiDiscretizer(
            List<DeterministicPolicyGroup<IN>> policyGroups,
            ReduceFunction<IN> reduceFunction, IN identityValue, int capacity, TypeSerializer<IN> serializer) {

        this.policyGroups = policyGroups;
        this.serializer = serializer;
        this.queryBorders = new HashMap<Integer, Deque<Integer>>();
        this.partialDependencies = new HashMap<Integer, Integer>();
        this.aggregator = new EagerHeapAggregator<IN>(reduceFunction, serializer, identityValue, capacity);
        this.reducer = reduceFunction;

        for (int i = 0; i < this.policyGroups.size(); i++) {
            queryBorders.put(i, new LinkedList<Integer>());
        }

        this.identityValue = identityValue;
        this.currentPartial = identityValue;

        chainingStrategy = ChainingStrategy.ALWAYS;
    }


    /**
     * For each tuple it does the following:
     * 
     * 1) registers the current partial to the WindowAggregator if any window begin event is invoked by the policy groups
     * with the exception of the first partial which is never used
     * 
     * 2) for each trigger event invoked by the policy groups it emits the full window aggregation via the collector 
     * by combining the pre-aggregated parts from the WindowAggregator with the current partial
     * 
     * @param tuple
     * @throws Exception
     */
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

                if ((windowEvents >> 16) > 0) {
                    if (partialIdx != 0) {
                        LOG.info("ADDING PARTIAL {} with value {} ", partialIdx, currentPartial);
                        aggregator.add(partialIdx, currentPartial);
                    }
                    partialIdx++;
                    currentPartial = identityValue;
                }

                for (int j = 0; j < (windowEvents >> 16); j++) {
                    queryBorders.get(i).addLast(partialIdx);
                    updatePartial(partialIdx, true);
                }
                for (int j = 0; j < (windowEvents & 0xFFFF); j++) {
                    collectAggregate(i);
                }
            }
        }
        currentPartial = reducer.reduce(serializer.copy(currentPartial), tuple);
    }


    /**
     * It removes a reference for the given partial ID and garbage collects unused partials in FIFO order
     * when they are no longer needed
     * 
     * @param partialId
     * @throws Exception
     */
    private void unregisterPartial(int partialId) throws Exception {
        int next = updatePartial(partialId, false);
        if (partialId == partialGC) {
            List<Integer> gcBag = new ArrayList<Integer>();
            int gcIndx = partialGC;
            while (next == 0 && gcIndx < partialIdx) {
                LOG.info("REMOVING PARTIAL {}", gcIndx);
                partialDependencies.remove(gcIndx++);
                next = partialDependencies.get(gcIndx);
            }
            aggregator.remove(gcBag.toArray(new Integer[gcBag.size()]));
            partialGC = gcIndx;
        }
    }

    /**
     * It adds or removes a reference for the given partial ID
     * @param partialId
     * @param addition
     * @return
     */
    private int updatePartial(int partialId, boolean addition) {
        int dependencies = 1;
        if (!addition || partialDependencies.containsKey(partialId)) {
            dependencies = partialDependencies.get(partialId) + (addition ? 1 : -1);
        }
        partialDependencies.put(partialId, dependencies);

        return dependencies;
    }

    /**
     * It collects at the output the result of a full window computation by fetching the aggregate needed
     * from the WindowAggregator and combining with the currently running partial
     * 
     * @param queryId
     * @throws Exception
     */
    private void collectAggregate(int queryId) throws Exception {
        Integer partial = queryBorders.get(queryId).getFirst();
        LOG.info("Q{} Emitting window from partial id: {}", queryId, partial);
        output.collect(new Tuple2<Integer, IN>(queryId, reducer.reduce(serializer.copy(aggregator.aggregate(partial)),
                serializer.copy(currentPartial))));
        queryBorders.get(queryId).removeFirst();
        unregisterPartial(partial);
    }

}
