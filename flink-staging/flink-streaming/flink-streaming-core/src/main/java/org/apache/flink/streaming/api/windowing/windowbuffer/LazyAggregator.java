package org.apache.flink.streaming.api.windowing.windowbuffer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * A lazy Aggregator keeps a simple list of partials and applies only final aggregations lazily upon request,
 * namely when aggregate functions are call. For n number of partials, that yields a O(n) aggregation complexity
 * and O(1) add/remove complexity. Furthermore, the space complexity of the LazyAggregator is O(n).
 *
 * @param <T>
 */
public class LazyAggregator<T> implements WindowAggregator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(EagerHeapAggregator.class);

    private final ReduceFunction<T> reduceFunction;
    private final TypeSerializer<T> serializer;
    private final T identityValue;

    private Map<Integer, Integer> partialMappings;
    private List<T> buffer;
    private int partialSpace;

    private int back, front;


    public LazyAggregator(ReduceFunction<T> reduceFunction, TypeSerializer<T> serializer, T identityValue, int initialCapacity) {
        this.reduceFunction = reduceFunction;
        this.serializer = serializer;
        this.identityValue = identityValue;
        this.partialSpace = initialCapacity;

        partialMappings = new LinkedHashMap<Integer, Integer>(partialSpace);
        buffer = new ArrayList<T>(Collections.nCopies(partialSpace, identityValue));
        back = -1;
        front = 0;
    }

    private void resize(int newSpace) {
        LOG.info("RESIZING BUFFER TO {}", newSpace);
        List<T> newBuffer = new ArrayList<T>(Collections.nCopies(newSpace, identityValue));
        int updateCount = 0;
        int indx = -1;
        for (Map.Entry<Integer, Integer> entry : partialMappings.entrySet()) {
            newBuffer.set(++indx, buffer.get(entry.getValue()));
            entry.setValue(indx);
        }
        this.front = 0;
        this.back = partialMappings.size()-1;
        this.partialSpace = newSpace;
        this.buffer = newBuffer;
    }

    @Override
    public void add(int id, T val) throws Exception {
        if (currentCapacity() == 0) {
            resize(2 * partialSpace);
        }

        incrBack();
        partialMappings.put(id, back);
        buffer.set(back, val);
    }

    @Override
    public void add(List<Integer> ids, List<T> vals) throws Exception {
        if (ids.size() != vals.size()) throw new IllegalArgumentException("The ids and vals given do not match");

        if (ids.size() > currentCapacity()) {
            int newCapacity = partialSpace;
            while (newCapacity < partialSpace - currentCapacity() + ids.size()) {
                newCapacity = 2 * newCapacity;
            }
            resize(newCapacity);
        }

        for (int i = 0; i < ids.size(); i++) {
            add(ids.get(i), vals.get(i));
        }
    }

    @Override
    public void remove(Integer... ids) throws Exception {
        for (int partialId : ids) {
            if (!partialMappings.containsKey(partialId)) continue;
            int leafID = partialMappings.get(partialId);
            partialMappings.remove(partialId);
            if (leafID != front) throw new IllegalArgumentException("Cannot evict out of order");
            buffer.set(front, identityValue);
            incrFront();
        }
        if (currentCapacity() > 3 * partialSpace / 4 && partialSpace >= 4) {
            resize(partialSpace / 2);
        }
    }

    @Override
    public T aggregate(int startid) throws Exception {
        int startIndx = partialMappings.get(startid);
        if(startIndx<=front){
            T partial = identityValue;
            for(int i=startIndx; i<=front; i++){
                partial = combine(partial, buffer.get(i));
            }
            return partial;
        }
        else {
            return combine(suffix(startIndx), prefix(front));
        }
    }

    @Override
    public T aggregate() throws Exception {
        return suffix(0);
    }

    private T suffix(int startId) throws Exception {
        T partial = identityValue;
        for (int i = startId; i < buffer.size(); i++) {
            partial = combine(partial, buffer.get(i));
        }
        return partial;
    }
    
    private T prefix(int endId) throws Exception {
        T partial = identityValue;
        for (int i = 0; i < endId; i++) {
            partial = combine(partial, buffer.get(i));
        }
        return partial;
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

    private int currentCapacity() {
        LOG.info("CURRENT CAPACITY : {} ");
        return partialSpace - partialMappings.size();
    }

    private int incrBack() {
        back = (back + 1) % partialSpace;
        return back;
    }

    private int incrFront() {
        front = (front + 1) % partialSpace;
        return front;
    }
}
