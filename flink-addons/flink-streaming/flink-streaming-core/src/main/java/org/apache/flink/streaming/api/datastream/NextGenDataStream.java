package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.operator.NextGenEvictionPolicy;
import org.apache.flink.streaming.api.invokable.operator.NextGenTriggerPolicy;
import org.apache.flink.streaming.api.invokable.operator.NextGenWindowingInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;
import org.apache.flink.streaming.util.serialization.ObjectTypeWrapper;

import java.util.LinkedList;

public class NextGenDataStream<OUT> {

    private static final String FUNCTION_NAME = "TEST";

    private LinkedList<NextGenTriggerPolicy<OUT>> triggerPolicies;
    private LinkedList<NextGenEvictionPolicy<OUT>> evictionPolicies;
    private DataStream<OUT> dataStream;
    private OUT sampleOUT;

    public NextGenDataStream(LinkedList<NextGenTriggerPolicy<OUT>> triggerPolicies,LinkedList<NextGenEvictionPolicy<OUT>> evictionPolicies, DataStream<OUT> dataStream, OUT sampleOUT) {
        this.triggerPolicies = triggerPolicies;
        this.evictionPolicies = evictionPolicies;
        this.dataStream = dataStream;
        this.sampleOUT = sampleOUT;
    }

    public SingleOutputStreamOperator<Tuple2<OUT, String[]>, ?> reduce(ReduceFunction<OUT> reducer) {

    	String[] sampleStringArray={""};
    	
        return dataStream.addFunction(FUNCTION_NAME, reducer,
                new FunctionTypeWrapper<OUT>(reducer, ReduceFunction.class, 0),
                new ObjectTypeWrapper<Tuple2<OUT, String[]>>(new Tuple2<OUT, String[]>(sampleOUT,sampleStringArray)),
                getReduceInvokable(reducer));
    }

    protected NextGenWindowingInvokable<OUT> getReduceInvokable(ReduceFunction<OUT> reducer) {
        return new NextGenWindowingInvokable<>(reducer,this.triggerPolicies,this.evictionPolicies);
    }
}
