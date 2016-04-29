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
import org.apache.flink.streaming.api.windowing.policy.TempPolicyGroup;
import org.apache.flink.streaming.api.windowing.windowbuffer.AggregationStats;
import org.apache.flink.streaming.api.windowing.windowbuffer.EagerHeapAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.LazyAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowAggregator;
import org.apache.flink.streaming.paper.AggregationFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

@SuppressWarnings("unused")
public class B2BMultiDiscretizer<IN, AGG extends Serializable> extends
        AbstractStreamOperator<Tuple2<Integer, AGG>> implements
        OneInputStreamOperator<Tuple2<IN, AGG>, Tuple2<Integer, AGG>> {

    private static final Logger LOG = LoggerFactory.getLogger(B2BMultiDiscretizer.class);

    private AggregationStats stats = AggregationStats.getInstance();
    
    /**
     * A user given reduce function used for continuously combining pre-aggregates
     */
    private final ReduceFunction<AGG> reducer;
    /**
     * The identity value for the given combine function where reduce(val, identity) == reduce(identity, val) == val
     */
    private final AGG identityValue;
    /**
     * All the policy groups that are co-located in one multi-discretizer
     */
    protected List<? extends DeterministicPolicyGroup<IN>> policyGroups;
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
    private WindowAggregator<AGG> aggregator;
    /**
     * A serializer used for copying values for immutability
     */
    private final TypeSerializer<AGG> serializer;

    /**
     * The partial ID index counter
     */
    private int partialIdx = 0;
    /**
     * The current inter-border aggregate
     */
    private AGG currentPartial;
	int errorcounter = 0;
	
	private long cnt;


	public B2BMultiDiscretizer(
            List<DeterministicPolicyGroup<IN>> policyGroups,
            ReduceFunction<AGG> reduceFunction, AGG identityValue, int capacity, TypeSerializer<AGG> serializer,
            AggregationFramework.AGGREGATION_STRATEGY aggregationType) {

        this.policyGroups = policyGroups;
        this.serializer = serializer;
        this.queryBorders = new HashMap<>();
        this.partialDependencies = new HashMap<>();
        this.reducer = reduceFunction;

        for (int i = 0; i < this.policyGroups.size(); i++) {
            queryBorders.put(i, new LinkedList<>());
        }

        this.identityValue = identityValue;
        this.currentPartial = identityValue;

        switch (aggregationType) {
            case EAGER:
                this.aggregator = new EagerHeapAggregator<>(reduceFunction, serializer, identityValue, capacity);
                break;
            case LAZY:
                this.aggregator = new LazyAggregator<>(reduceFunction, serializer, identityValue, capacity);
        }

        chainingStrategy = ChainingStrategy.ALWAYS;
    }

	


	/**
     * For each tuple it does the following:
     * <p/>
     * 1) registers the current partial to the WindowAggregator if any window begin event is invoked by the policy groups
     * with the exception of the first partial which is never used
     * <p/>
     * 2) for each trigger event invoked by the policy groups it emits the full window aggregation via the collector
     * by combining the pre-aggregated parts from the WindowAggregator with the current partial
     *
     * @param tuple
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Override
    public void processElement(Tuple2<IN, AGG> tuple) throws Exception {
        // First handle the deterministic policies
		stats.startRecord();
        boolean partialUpdated = false;
        for (int i = 0; i < policyGroups.size(); i++) {
			stats.setAggregationMode(AggregationStats.AGGREGATION_MODE.UPDATES);
			int windowEvents = policyGroups.get(i).getWindowEvents(tuple.f0);
			
			int starts = windowEvents >> 16;
			int ends = windowEvents & 0xFFFF;
			
            if (windowEvents != 0) {

                // **STRATEGY FOR REGISTERING PARTIALS**
                // 1) first partial does not need to be added in the pre-aggregation buffer
                // 2) we only need to add eviction borders in the pre-aggregation buffer - consecutive triggers
                //    can reuse the current partial on-the-fly (no need to pre-aggregate that)!
				
				if (starts > 0 && !partialUpdated) {
					stats.registerPartial();
                    if (partialIdx != 0) {
                        LOG.debug("ADDING PARTIAL {} with value {} ", partialIdx, currentPartial);
						stats.registerStartUpdate();
						aggregator.add(partialIdx, currentPartial);
						stats.registerEndUpdate();
					}
                    partialIdx++;
                    currentPartial = identityValue;
                    partialUpdated = true;
                }

                for (int j = 0; j < starts; j++) {
                    queryBorders.get(i).addLast(partialIdx);
                    updatePartial(partialIdx, true);
                }
				for (int j = 0; j < ends; j++) {
					if(policyGroups.get(i) instanceof TempPolicyGroup){
						//avoid additional merges in the case of pairs
						Integer partial = queryBorders.get(i).pollFirst();
						try {
							unregisterPartial(partial);
						}catch(Exception ex){
							errorcounter++;
							System.err.println("Unregistering inexistent partial :"+partial+" - error count : "+errorcounter);
						}
					}
					else {
						stats.registerStartMerge();
						stats.setAggregationMode(AggregationStats.AGGREGATION_MODE.AGGREGATES);
						collectAggregate(i);
						stats.registerEndMerge();
					}
				}
            }
        }
		stats.registerStartUpdate();
        currentPartial = reducer.reduce(serializer.copy(currentPartial), tuple.f1);
        stats.registerEndUpdate();
		stats.endRecord();
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
        if (next == 0 && partialId == partialGC) {
            List<Integer> gcBag = new ArrayList<Integer>();
            int gcIndx = partialGC;
            while (next == 0 && gcIndx < partialIdx) {
                gcBag.add(gcIndx);
                LOG.debug("REMOVING PARTIAL {}", gcIndx);
                partialDependencies.remove(gcIndx++);
                next = partialDependencies.get(gcIndx);
            }
            aggregator.remove(gcBag.toArray(new Integer[gcBag.size()]));
            partialGC = gcIndx;
        }
    }

    /**
     * It adds or removes a reference for the given partial ID
     *
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
        try {
			Integer partial = queryBorders.get(queryId).getFirst();
			LOG.info("Q{} Emitting window from partial id: {}", queryId, partial);
			output.collect(new Tuple2<>(queryId, reducer.reduce(serializer.copy(aggregator.aggregate(partial)),
					serializer.copy(currentPartial))));
			queryBorders.get(queryId).removeFirst();
			stats.setAggregationMode(AggregationStats.AGGREGATION_MODE.UPDATES);
			unregisterPartial(partial);
		}catch(Exception ex){
			LOG.error("FAILED TO AGGREGATE FOR QUERY : "+queryId);
		}
    }

}
