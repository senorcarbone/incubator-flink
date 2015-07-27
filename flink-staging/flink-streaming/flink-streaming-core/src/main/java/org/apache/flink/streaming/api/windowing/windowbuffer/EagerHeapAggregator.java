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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * A full aggregator computes all pre-aggregates per partial result added. This implementation yields
 * the best aggregateFrom performance but it also has a heavy add operation. It is based on the FlatFat implementation
 * described by Kanat et al. in "General Incremental Sliding-Window Aggregation" published in VLDB 15. It encodes window
 * pre-aggregates in a binary tree using a fixed-size circular heap with implicit node relations.
 * <p/>
 * Node relations follow a typical heap structure. The heap is structured as such for example:
 * <p/>
 * | root | left(root) | right(root) | left(left(root)) ... | P1 | P2 | P3 | ... | Pn |
 * Back      Head
 * <p/>
 * The leaf space allocated for partials Pi lies within n-1 and 2n-2 indexes.
 * Furthermore we maintain a front and back pointer that circulate within the leaf space to mark the current
 * partial buffer. We always add new elements on the back and remove from the front in FIFO order.
 * <p/>
 * <p/>
 * The space complexity of this implementation is, for n partial aggregates 2n-1. Furthermore, it also keeps an index of
 * n partial aggregate ids.
 *
 * @param <T>
 */
public class EagerHeapAggregator<T> implements WindowAggregator<T>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(EagerHeapAggregator.class);
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
     * @param identityValue  as the identity value (i.e. where reduce(defVal, val) == reduce(val, defVal) == val)
     * @param capacity
     */
    public EagerHeapAggregator(ReduceFunction<T> reduceFunction, TypeSerializer<T> serializer, T identityValue, int capacity) {
        this.reduceFunction = reduceFunction;
        this.serializer = serializer;
        this.identityValue = identityValue;
        this.numLeaves = capacity;
        this.back = capacity - 2;
        this.front = capacity - 1;
        this.leafIndex = new LinkedHashMap<Integer, Integer>(capacity);

        int fullCapacity = 2 * capacity - 1;
        this.circularHeap = new ArrayList<T>(Collections.nCopies(fullCapacity, identityValue));
    }

    @Override
    public void add(int partialId, T partialVal) throws Exception {
        add(partialId, partialVal, true);
    }

    /**
     * It adds the given value in the leaf space and if commit is set to true it materializes all partial pre-aggregates
     *
     * @param partialId
     * @param partialVal
     * @param commit
     * @throws Exception
     */
    private int add(int partialId, T partialVal, boolean commit) throws Exception {
        if (currentCapacity() == 0) {
            LOG.info("Resizing heap to {}" + 2 * numLeaves);
            resize(2 * numLeaves);
        }
        incrBack();
        leafIndex.put(partialId, back);
        circularHeap.set(back, serializer.copy(partialVal));
        if (commit) {
            update(back);
        }

        return back;
    }

    /**
     * It reconstructs the heap with a new leaf space of size newCapacity
     *
     * @param newCapacity
     */
    private void resize(int newCapacity) throws Exception {
        List<T> newHeap = new ArrayList<T>(Collections.nCopies(2 * newCapacity - 1, identityValue));
        Integer[] updated = new Integer[leafIndex.size()];
        int indx = newCapacity - 2;
        int updateCount = 0;
        for (Map.Entry<Integer, Integer> entry : leafIndex.entrySet()) {
            newHeap.set(++indx, circularHeap.get(entry.getValue()));
            entry.setValue(indx);
            updated[updateCount++] = indx;
        }
        this.numLeaves = newCapacity;
        this.back = indx;
        this.front = newCapacity - 1;
        this.circularHeap = newHeap;
        update(updated);
    }

    @Override
    public void add(List<Integer> ids, List<T> vals) throws Exception {
        if (ids.size() != vals.size()) throw new IllegalArgumentException("The ids and vals given do not match");

        //if we have reached max capacity (numLeaves) resize so that i*numLeaves > usedSpace+newSpace
        if (ids.size() > currentCapacity()) {
            int newCapacity = numLeaves;
            while (newCapacity < numLeaves - currentCapacity() + ids.size()) {
                newCapacity = 2 * newCapacity;
            }
            resize(newCapacity);
        }

        //add all new leaf values and perform a bulk update
        Integer[] updatedLeaves = new Integer[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            updatedLeaves[i] = add(ids.get(i), vals.get(i), false);
        }
        update(updatedLeaves);
    }

    @Override
    public void remove(Integer... partialList) throws Exception {
        List<Integer> leafBag = new ArrayList<Integer>(partialList.length);
        for (int partialId : partialList) {
            if (!leafIndex.containsKey(partialId)) continue;
            int leafID = leafIndex.get(partialId);
            if (leafID != front) throw new IllegalArgumentException("Cannot evict out of order");
            leafIndex.remove(leafID);
            circularHeap.set(front, identityValue);
            incrFront();
            leafBag.add(leafID);
        }
        update(leafBag.toArray(new Integer[leafBag.size()]));
        // shrink to half when the utilization is only one quarter
        if (currentCapacity() >= 3 * numLeaves / 4) {
            resize(numLeaves / 2);
        }
    }

    @Override
    public T aggregate(int partialId) throws Exception {
        if (leafIndex.containsKey(partialId)) {
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
     * Applies eager bulk pre-aggregation for all given mutated leafIDs. This works exactly as described in the RA paper
     */
    private void update(Integer... leafIds) throws Exception {
        Set<Integer> next = Sets.newHashSet(leafIds);
        do {
            Set<Integer> tmp = new HashSet<Integer>();
            for (Integer nodeId : next) {
                if (nodeId != ROOT) {
                    tmp.add(parent(nodeId));
                }
            }
            for (Integer parent : tmp) {
                circularHeap.set(parent, combine(circularHeap.get(left(parent)), circularHeap.get(right(parent))));
            }
            next = tmp;
        } while (!next.isEmpty());
    }

    /**
     * It invokes a reduce operation on copies of the given values
     *
     * @param val1
     * @param val2
     * @return
     * @throws Exception
     */
    private T combine(T val1, T val2) throws Exception {
        return reduceFunction.reduce(serializer.copy(val1), serializer.copy(val2));
    }

    /**
     * It collects an aggregated result starting from the leafID given until the back index of the circular heap
     *
     * @param leafID
     * @return
     * @throws Exception
     */
    private T aggregateFrom(int leafID) throws Exception {
        if (back < leafID) {
            return combine(prefix(back), suffix(leafID));
        }

        return (leafID == front) ? circularHeap.get(ROOT) : suffix(leafID);
    }

    /**
     * it collects an aggregated result starting from the leafID given until the end of the leaf space
     *
     * @param leafID
     * @return
     * @throws Exception
     */
    private T suffix(int leafID) throws Exception {
        int next = leafID;
        T agg = circularHeap.get(next);
        while (next != ROOT) {
            int p = parent(next);
            if (next == left(p)) {
                agg = combine(agg, circularHeap.get(right(p)));
            }

            next = p;
        }

        return agg;
    }

    /**
     * it collects an aggregated result from the beginning of the leaf space to the leafID given
     *
     * @param leafId
     * @return
     * @throws Exception
     */
    private T prefix(int leafId) throws Exception {
        int next = leafId;
        T agg = circularHeap.get(next);
        while (next != ROOT) {
            int p = parent(next);
            if (next == right(p)) {
                agg = combine(circularHeap.get(left(p)), agg);
            }

            next = p;
        }

        return agg;
    }

    /**
     * It returns the parent of nodeID from the heap space
     *
     * @param nodeId
     * @return
     */
    private int parent(int nodeId) {
        return (nodeId - 1) / 2;
    }

    /**
     * It returns the left child of the nodeID given
     *
     * @param nodeId
     * @return
     */
    private int left(int nodeId) {
        return 2 * nodeId + 1;
    }

    /**
     * It returns the right child of the nodeID given
     *
     * @param nodeId
     * @return
     */
    private int right(int nodeId) {
        return 2 * nodeId + 2;
    }

    /**
     * It moves the back pointer of the leaf space forward in the circular buffer
     */
    private void incrBack() {
        back = ((back - numLeaves + 2) % numLeaves) + numLeaves - 1;
    }

    /**
     * It moves the front pointer of the leaf space forward in the circular buffer
     */
    private void incrFront() {
        front = ((front - numLeaves + 2) % numLeaves) + numLeaves - 1;
    }

    /**
     * @return the current number of free slots in the leaf space
     */
    private int currentCapacity() {
        int diff = back - front + 1;
        if (diff == 0) {
            return 0;
        } else if (diff < 0) {
            return numLeaves - front - back;
        } else {
            return numLeaves - diff;
        }
    }

}
