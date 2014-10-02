package org.apache.flink.streaming.api.invokable.operator;

import com.amazonaws.services.sqs.model.UnsupportedOperationException;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class NextGenWindowingInvokable<IN, FLAG> extends StreamInvokable<IN, Tuple2<IN, Object[]>> {

    /**
     * Auto-generated serial version UID
     */
    private static final long serialVersionUID = -8038984294071650730L;

    private LinkedList<NextGenPolicy<IN>> triggerPolicies;
    private LinkedList<NextGenPolicy<IN>> emitPolicies;
    private NextGenWindowType windowType;
    /*
     * TODO A LinkedList is obviously not the best possible way of buffering ;)
     *      Any ordered collection with flexible size might do the job as well.
     *      In any case it doesn't make sense to always wait with the start of
     *      reduce function until a window is full. We need something like a
     *      combiner in hadoop m/r.
     */
    private List<IN> buffer = new LinkedList<>();
    private LinkedList<NextGenPolicy<IN>> currentEmittingPolicies = new LinkedList<>();
    private ReduceFunction<IN> reducer;

    public NextGenWindowingInvokable(
            ReduceFunction<IN> userFunction,
            LinkedList<NextGenPolicy<IN>> triggerPolicies,
            LinkedList<NextGenPolicy<IN>> emitPolicies,
            NextGenWindowType windowType
    ) {
        super(userFunction);

        this.reducer = userFunction;
        this.triggerPolicies = triggerPolicies;
        this.emitPolicies = emitPolicies;

        //TODO implement support for sliding windows
        if (windowType != NextGenWindowType.TUMBLING) {
            throw new UnsupportedOperationException("Only tumbling windows are implemented at the moment!");
        }

        this.windowType = windowType;
    }

    @Override
    protected void immutableInvoke() throws Exception {
        //Prevent empty data streams
        if ((reuse = recordIterator.next(reuse)) == null) {
            throw new RuntimeException("DataStream must not be empty");
        }

        while (reuse != null) {

            //TODO There is no implementation for SLIDING windows right now.
            //     Therefore, the trigger policy code is dead somehow...
            if (this.windowType == NextGenWindowType.SLIDING) {
                //Process the triggers (in case of multiple triggers compute only once!)
                for (NextGenPolicy<IN> triggerPolicy : triggerPolicies) {
                    if (triggerPolicy.addDataPoint(reuse.getObject())) {
                        //TODO trigger

                        //prevent multiple triggering on the same status as it would be computation of the same result again
                        break;
                    }
                }
            }

            //Process the emissions and take care of double emissions
            for (NextGenPolicy<IN> emitPolicy : emitPolicies) {
                if (emitPolicy.addDataPoint(reuse.getObject())) {
                    currentEmittingPolicies.add(emitPolicy);
                }
            }
            if (!currentEmittingPolicies.isEmpty()) {
                //emit
                callUserFunctionAndLogException();

                //clear the flag collection
                currentEmittingPolicies.clear();
                //clear the data buffer
                buffer.clear();
            }

            //Add the current element to the buffer
            buffer.add(reuse.getObject());

            //Recreate the reuse-StremRecord object and load next StreamRecord
            resetReuse();
            reuse = recordIterator.next(reuse);
        }

        //TODO finally trigger?!

        //finally emit the buffer content and set all policies as emitted...
        if (!buffer.isEmpty()) {
            currentEmittingPolicies.clear();
            for (NextGenPolicy<IN> policy : emitPolicies) {
                currentEmittingPolicies.add(policy);
            }
            callUserFunctionAndLogException();
        }

    }

    @Override
    protected void mutableInvoke() throws Exception {
        // TODO implement mutable version
        throw new UnsupportedOperationException("There is no mutable implementation at the moment!");
    }

    @Override
    protected void callUserFunction() throws Exception {
        Iterator<IN> reducedIterator = buffer.iterator();
        IN reduced = null;

        while (reducedIterator.hasNext() && reduced == null) {
            reduced = reducedIterator.next();
        }

        while (reducedIterator.hasNext()) {
            IN next = reducedIterator.next();
            if (next != null) {
                reduced = reducer.reduce(reduced, next);
            }
        }
        if (reduced != null) {
            collector.collect(new Tuple2<IN, Object[]>(reduced, currentEmittingPolicies.toArray()));
        }
    }

}
