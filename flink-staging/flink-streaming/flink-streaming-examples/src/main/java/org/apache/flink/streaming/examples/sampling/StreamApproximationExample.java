package org.apache.flink.streaming.examples.sampling;

import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.uncommons.maths.random.GaussianGenerator;
//import org.apache.commons.math3.distribution.*;

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

    private static DataStream<Double> createGaussianEvolvingStream(StreamExecutionEnvironment env, final int length) {
        final double mean1 = 0;
        final double mean2 = 10;
        final double stdev1 = 1;
        final double stdev2 = 3;
        final Random rand = new Random();
        //TODO
        DataStream<Long> stream = env.generateSequence(1, length);
        return stream.map(new MapFunction<Long, Double>() {
            long counter = 0;
            @Override
            public Double map(Long value) throws Exception {
                double meanStep = (mean2-mean1)/length;
                double stdevStep = (stdev2-stdev1)/length;
                double nextMean = mean1 + counter*meanStep;
                double nextStdev = stdev1 + counter*stdevStep;
                counter ++;
                return rand.nextGaussian()*nextStdev+nextMean;
            }
        });


    }


}
