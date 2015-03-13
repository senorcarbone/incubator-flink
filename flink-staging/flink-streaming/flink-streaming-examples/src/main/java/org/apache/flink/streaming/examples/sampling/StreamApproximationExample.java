package org.apache.flink.streaming.examples.sampling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

/**
 * Created by marthavk on 2015-03-13.
 */
public class StreamApproximationExample {

    public StreamApproximationExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create changing rng src
        // reservoir sampling component
        // evaluation component


        env.execute();
    }

    public static void main(String args[]) throws Exception {
        new StreamApproximationExample();
    }

    private static DataStream<Double> createGaussianStream(StreamExecutionEnvironment env, int length) {
        DataStream<Long> stream = env.generateSequence(1, length);
        return stream.map(new MapFunction<Long, Double>() {
            @Override
            public Double map(Long value) throws Exception {
                Random rand = new Random();
                return rand.nextGaussian();
            }
        });
    }

    private static DataStream<Double> createGaussianEvolvingStream(StreamApproximationExample env, int length) {
        //TODO
        return null;
    }


}
