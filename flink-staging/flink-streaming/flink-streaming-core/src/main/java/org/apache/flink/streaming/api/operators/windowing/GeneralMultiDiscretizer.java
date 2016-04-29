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
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.AggregationStats;
import org.apache.flink.streaming.api.windowing.windowbuffer.EagerHeapAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.LazyAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowAggregator;
import org.apache.flink.streaming.paper.AggregationFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


@SuppressWarnings("unused")
public class GeneralMultiDiscretizer<IN, AGG> extends
        AbstractStreamOperator<Tuple2<Integer, AGG>> implements
        OneInputStreamOperator<Tuple2<IN, AGG>, Tuple2<Integer, AGG>> {

    private static final Logger LOG = LoggerFactory
            .getLogger(GeneralMultiDiscretizer.class);

    private AggregationStats stats = AggregationStats.getInstance();

    private static final int DEFAULT_CAPACITY = 16;

    private HashMap<Integer, LinkedList<Integer>> queryIdToWindowIds = new HashMap<Integer, LinkedList<Integer>>();
    private List<TriggerPolicy<IN>> triggerPolicies;
    private List<EvictionPolicy<IN>> evictionPolicies;
    private List<Integer> queryBorders;
    private WindowAggregator<AGG> aggregator;
    /**
     * A serializer used for copying values for immutability
     */
    private final TypeSerializer<AGG> serializer;
    /**
     * The identity value for the given combine function where reduce(val, identity) == reduce(identity, val) == val
     */
    private final AGG identityValue;

    private int recordCounter = 0;

    public GeneralMultiDiscretizer(
            List<TriggerPolicy<IN>> triggerPolicies,
            List<EvictionPolicy<IN>> evictionPolicies,
            ReduceFunction<AGG> reduceFunction, TypeSerializer<AGG> serializer, AGG identityValue, AggregationFramework.AGGREGATION_STRATEGY aggType) {
        if (triggerPolicies.size() != evictionPolicies.size())
            throw new IllegalArgumentException("Trigger and Eviction Policies should match");
        this.evictionPolicies = evictionPolicies;
        this.triggerPolicies = triggerPolicies;
        this.serializer = serializer;
        this.identityValue = identityValue;
        this.queryBorders = new ArrayList<>(Collections.nCopies(triggerPolicies.size(), 1));
        switch (aggType) {
            case EAGER:
                this.aggregator = new EagerHeapAggregator<>(reduceFunction, serializer, identityValue, DEFAULT_CAPACITY);
                break;
            case LAZY:
                this.aggregator = new LazyAggregator<>(reduceFunction, this.serializer, this.identityValue, DEFAULT_CAPACITY);
        }
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processElement(Tuple2<IN, AGG> tuple) throws Exception {
		stats.startRecord();
        boolean hasEvicted = false;
        for (int i = 0; i < triggerPolicies.size(); i++) {
			stats.setAggregationMode(AggregationStats.AGGREGATION_MODE.UPDATES);
            if (triggerPolicies.get(i) instanceof ActiveTriggerPolicy) {
                Object[] preNotificationTuples = ((ActiveTriggerPolicy) triggerPolicies
                        .get(i)).preNotifyTrigger(tuple.f0);
                for (Object preNotificationTuple : preNotificationTuples) {
                    if (evictionPolicies.get(i) instanceof ActiveEvictionPolicy) {
                        evict(i, ((ActiveEvictionPolicy) evictionPolicies.get(i)).notifyEvictionWithFakeElement(
                                preNotificationTuple, recordCounter-queryBorders.get(i)+1));
                    }
                    stats.registerStartMerge();
                    emitWindow(i);
                    stats.registerEndMerge();
                }
            }
            
            if (triggerPolicies.get(i).notifyTrigger(tuple.f0)) {
                stats.registerStartMerge();
                emitWindow(i);
                stats.registerEndMerge();
                int evicted = evictionPolicies.get(i).notifyEviction(tuple.f0, true, recordCounter - queryBorders.get(i) + 1);
                hasEvicted = evicted > 0;
                evict(i, evicted);
            } else {
                int evicted = evictionPolicies.get(i).notifyEviction(tuple.f0, false, recordCounter - queryBorders.get(i) + 1);
                evict(i, evicted);
                hasEvicted = evicted > 0;
            }
        }
        if (hasEvicted) {
			stats.setAggregationMode(AggregationStats.AGGREGATION_MODE.UPDATES);
            aggregator.removeUpTo(Collections.min(queryBorders));
        }
        
        stats.registerStartUpdate();
        store(tuple.f1);
        stats.registerEndUpdate();
		stats.endRecord();
    }


    private void store(AGG tuple) throws Exception {
		stats.setAggregationMode(AggregationStats.AGGREGATION_MODE.UPDATES);
        if (++recordCounter == Integer.MAX_VALUE) {  //FIXME handle this properly
            throw new RuntimeException("The sequence id reached the limit given by the type long!");
        }
		stats.registerPartial();
		aggregator.add(recordCounter, tuple);
    }

    private void emitWindow(int queryId) throws Exception {
		stats.setAggregationMode(AggregationStats.AGGREGATION_MODE.AGGREGATES);
        LOG.info("Aggregation for Q{} from {}", queryId, queryBorders.get(queryId));
        output.collect(new Tuple2<>(queryId, aggregator.aggregate(queryBorders.get(queryId))));
    }

    private void evict(int queryId, int n) {
        if (n > 0) {
            queryBorders.set(queryId, queryBorders.get(queryId) + n);
        }
    }
}
