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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;
import java.util.*;

/**
 * A full aggregator computes all pre-aggregates per partial result added. This implementation yields
 * the best aggregateFrom performance but it also has a heavy add operation. It is based on the FlatFat implementation
 * described by Kanat et al. in "General Incremental Sliding-Window Aggregation" published in VLDB 15. It encodes window
 * pre-aggregates in a binary tree using a fixed-size circular heap with implicit node relations.
 * 
 *
 * The space complexity of this implementation is, for n partial aggregates 2n-1. Furthermore, it also keeps an index of
 * n partial aggregate ids.
 * 
 * @param <T>
 */
public class EagerHeapAggregator<T> implements WindowAggregator<T>, Serializable {

    private final ReduceFunction<T> reduceFunction;
    private final TypeSerializer<T> serializer;
    private final T identityValue;


    private Map<Integer, Integer> leafIndex;
    /**
     * We use a fixed size list for the circular heap. We did not use an array due to the usual generics
     * issue in Java. Performance should be comparably reasonable using an ArrayList implementation.
     */
    private List<T> circularHeap;

    private int numLeaves;
    private int back, front;
    private final int ROOT = 0;

    /**
     * @param reduceFunction
     * @param serializer
     * @param identityValue        as the identity value (i.e. where reduce(defVal, val) == reduce(val, defVal) == val)
     * @param capacity
     */
    public EagerHeapAggregator(ReduceFunction<T> reduceFunction, TypeSerializer<T> serializer, T identityValue, int capacity) {
        this.reduceFunction = reduceFunction;
        this.serializer = serializer;
        this.identityValue = identityValue;
        this.numLeaves = capacity;
        this.back = capacity-2;
        this.front = capacity-1;
        this.leafIndex = new HashMap<Integer, Integer>(capacity);

        int fullCapacity = 2 * capacity - 1;
        this.circularHeap = new ArrayList<T>(Collections.nCopies(fullCapacity, identityValue));
    }

    @Override
    public void add(int partialId, T partialVal) throws Exception {
        incrBack();
        leafIndex.put(partialId, back);
        circularHeap.set(back, serializer.copy(partialVal));
        update(back);
    }

    @Override
    public void remove(int partialId) throws Exception {
        int leafID = leafIndex.get(partialId);
        if (leafID != front) throw new IllegalArgumentException("Cannot evict out of order");
        leafIndex.remove(partialId);
        circularHeap.set(front, identityValue);
        update(front);
        incrFront();
    }

    @Override
    public T aggregate(int partialId) throws Exception {
        if(leafIndex.containsKey(partialId))
        {
            return aggregateFrom(leafIndex.get(partialId));
        }
        // in case no partials are registered (can be true if we trigger in the very beginning) return the identity
        return identityValue; 
    }

    @Override
    public T aggregate() throws Exception {
        return aggregateFrom(front);
    }

    /**
     * Applies eager bulk pre-aggregation for all given mutated node Ids
     */
    private void update(Integer... leafIds) throws Exception {
        Set<Integer> next = Sets.newHashSet(leafIds);
        do {
            Set<Integer> tmp = new HashSet<Integer>();
            for(Integer nodeId : next) {
                if(nodeId != ROOT){
                    tmp.add(parent(nodeId));
                }
            }
            for(Integer parent : tmp){
                circularHeap.set(parent, combine(circularHeap.get(left(parent)), circularHeap.get(right(parent))));
            }
            next = tmp;
        } while (!next.isEmpty());
    }

    private T combine(T val1, T val2) throws Exception {
      return reduceFunction.reduce(serializer.copy(val1), serializer.copy(val2));
    }

    private T aggregateFrom(int nodeId) throws Exception {
        if (back < nodeId) {
            return combine(prefix(back), suffix(nodeId));
        }

        return (nodeId == front) ? circularHeap.get(ROOT) : suffix(nodeId);
    }

    private T suffix(int nodeId) throws Exception {
        int next = nodeId;
        T agg = circularHeap.get(next);
        while(next != ROOT) {
            int p = parent(next);
            if(next == left(p)){
                agg = combine(agg,circularHeap.get(right(p)));
            }

            next = p;
        }

        return agg; 
    }

    private T prefix(int nodeId) throws Exception {
        int next = nodeId;
        T agg = circularHeap.get(next);
        while(next != ROOT) {
            int p = parent(next);
            if(next == right(p)){
                agg = combine(circularHeap.get(left(p)), agg);
            }
            
            next = p;
        }
        
        return agg;
    }

    private int parent(int nodeId) {
        return (nodeId-1) / 2;
    }

    private int left(int nodeId) {
        return 2 * nodeId + 1;
    }

    private int right(int nodeId) {
        return 2 * nodeId + 2;
    }

    private void incrBack() {
        back = ((back - numLeaves + 2) % numLeaves) + numLeaves-1;
    }

    private void incrFront() {
        front = ((front - numLeaves + 2) % numLeaves) + numLeaves-1;
    }

}
