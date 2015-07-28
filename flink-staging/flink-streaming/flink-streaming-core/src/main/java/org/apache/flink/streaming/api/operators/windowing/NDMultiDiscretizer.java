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
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.EagerHeapAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.LazyAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowAggregator;
import org.apache.flink.streaming.paper.AggregationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


@SuppressWarnings("unused")
public class NDMultiDiscretizer<IN> extends
        AbstractStreamOperator<Tuple2<Integer, IN>> implements
        OneInputStreamOperator<IN, Tuple2<Integer, IN>> {

    private static final Logger LOG = LoggerFactory
            .getLogger(NDMultiDiscretizer.class);

    private static final int DEFAULT_CAPACITY = 32;

    private HashMap<Integer, LinkedList<Integer>> queryIdToWindowIds = new HashMap<Integer, LinkedList<Integer>>();
    private List<TriggerPolicy<IN>> triggerPolicies;
    private List<EvictionPolicy<IN>> evictionPolicies;
    private int[] queryBorders;
    private WindowAggregator<IN> aggregator;
    /**
     * A serializer used for copying values for immutability
     */
    private final TypeSerializer<IN> serializer;
    /**
     * The identity value for the given combine function where reduce(val, identity) == reduce(identity, val) == val
     */
    private final IN identityValue;
    
    private int recordCounter = 0;


    public NDMultiDiscretizer(
            List<TriggerPolicy<IN>> triggerPolicies,
            List<EvictionPolicy<IN>> evictionPolicies,
            ReduceFunction<IN> reduceFunction, TypeSerializer<IN> serializer, IN identityValue, AggregationUtils.AGGREGATION_TYPE aggType) {
        this.evictionPolicies = evictionPolicies;
        this.triggerPolicies = triggerPolicies;
        this.serializer = serializer;
        this.identityValue = identityValue;
        this.queryBorders = new int[this.triggerPolicies.size()];
        switch (aggType) {
            case EAGER:
                this.aggregator = new EagerHeapAggregator<IN>(reduceFunction, serializer, identityValue, DEFAULT_CAPACITY);
                break;
            case LAZY:
                this.aggregator = new LazyAggregator<IN>(reduceFunction, this.serializer, this.identityValue, DEFAULT_CAPACITY);
        }
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processElement(IN tuple) throws Exception {

        for (int i = 0; i < triggerPolicies.size(); i++) {
            if (triggerPolicies.get(i).notifyTrigger(tuple)) {
                emitWindow(i);
                evict(i, evictionPolicies.get(i).notifyEviction(tuple, true, recordCounter - queryBorders[i]));
            } else {
                evict(i, evictionPolicies.get(i).notifyEviction(tuple, false, recordCounter - queryBorders[i]));
            }

        }
        //TODO ADD GARBAGE COLLECTION
        store(tuple);
    }

    private void store(IN tuple) throws Exception {
        recordCounter++;
        aggregator.add(recordCounter, tuple);
    }

    private void emitWindow(int queryId) throws Exception {
        LOG.info("Aggregation for Q{} from {}", queryId, queryBorders[queryId]);
        output.collect(new Tuple2<Integer, IN>(queryId, aggregator.aggregate(queryBorders[queryId])));
    }

    private void evict(int queryId, int n) {
        if (n > 0) {
            queryBorders[queryId] = queryBorders[queryId] + n;
        }
    }
}
