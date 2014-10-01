package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.operator.NextGenPolicy;
import org.apache.flink.streaming.api.invokable.operator.NextGenWindowType;
import org.apache.flink.streaming.api.invokable.operator.NextGenWindowingInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;
import org.apache.flink.streaming.util.serialization.ObjectTypeWrapper;

import java.util.LinkedList;

public class NextGenDataStream<OUT> {

    private static final String FUNCTION_NAME = "TEST";

    private LinkedList<NextGenPolicy<OUT, Integer>> emitPolicies;
    private DataStream<OUT> dataStream;
    private OUT sampleOUT;

    public NextGenDataStream(LinkedList<NextGenPolicy<OUT, Integer>> emitPolicies, DataStream<OUT> dataStream, OUT sampleOUT) {
        this.emitPolicies = emitPolicies;
        this.dataStream = dataStream;
        this.sampleOUT = sampleOUT;
    }

    public SingleOutputStreamOperator<Tuple2<OUT, LinkedList<Integer>>, ?> reduce(ReduceFunction<OUT> reducer) {

        //create the samples for the ObjectTypeWrapper
        LinkedList<Integer> sample = new LinkedList<Integer>();
        sample.add(1);

        FunctionTypeWrapper<OUT> objectFunctionTypeWrapper = new FunctionTypeWrapper<>(reducer, ReduceFunction.class, 0);
        ObjectTypeWrapper<Tuple2<OUT, LinkedList<Integer>>> outTypeWrapper = new ObjectTypeWrapper<>(new Tuple2<OUT, LinkedList<Integer>>(sampleOUT, sample));

        return dataStream.addFunction(FUNCTION_NAME, reducer, objectFunctionTypeWrapper, outTypeWrapper, getReduceInvokable(reducer));

    }

    protected NextGenWindowingInvokable<OUT, Integer> getReduceInvokable(ReduceFunction<OUT> reducer) {
        //TODO add support for trigger policies
        //TODO add support for sliding windows
        return new NextGenWindowingInvokable<>(reducer, null/*triggerPolicies*/, emitPolicies, NextGenWindowType.TUMBLING);
    }
}
