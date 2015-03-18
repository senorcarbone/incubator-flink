/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.sampling;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.*;


import java.io.Serializable;
import java.util.Random;

/**
 * Created by marthavk on 2015-03-13.
 */
public class StreamApproximationExample {

    public static long MAX_COUNT = 10000000;



    public StreamApproximationExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int reservoirSize = 10; //sample size
        int windowSize = 100; //size of the overlapping window
        int windowFrequency = 1; // frequency of new window trigger
        // create changing rng src
        //DataStream<Double> stream = createStreamFromFile(env);
        Parameters params = new Parameters(0, 1, 1, 0.1, 0);
        createStream(env, params)
                .shuffle()
                .print();

        // reservoir sampling component
        //windowReservoirSampling(stream, reservoirSize, windowSize, windowFrequency);
        // evaluation component


        env.execute();
    }

    public static void main(String args[]) throws Exception {
        new StreamApproximationExample();
    }

/*    private static DataStream<Double> createGaussianStream(StreamExecutionEnvironment env, int length) {
        DataStream<Long> stream = env.generateSequence(1, length);
        return stream.map(new MapFunction<Long, Double>() {
            @Override
            public Double map(Long value) throws Exception {
                Random rand = new Random();
                return rand.nextGaussian();
            }
        });
    }*/



    private DataStream<Reservoir<Double>> simpleReservoirSampling(DataStream<Double> dataStream, StreamExecutionEnvironment env, final int rSize) {
        return dataStream.map(new MapFunction<Double, Reservoir<Double>>() {
            Reservoir<Double> r = new Reservoir<Double>(rSize);
            int count = 0;

            @Override
            public Reservoir<Double> map(Double aDouble) throws Exception {
                count++;
                if (Coin.flip(count / rSize)) {
                    r.insertElement(aDouble);
                }
                return r;
            }

        });
    }

    private void evaluate(DataStream<Double> dataStream) {

    }





    private static DataStreamSource<Double> createStream(StreamExecutionEnvironment env, final Parameters params) {
        return env.addSource(new RichSourceFunction<Double>() {
            long count = 0;

            @Override
            public void run(Collector<Double> collector) throws Exception {

                while (count<MAX_COUNT) {
                    count++;
                    //generate random gaussian double
                    double newItem = Gaussian.nextGaussian(params.getCurrentMean(count), params.getCurrentStDev(count));
                    //collect new item
                    /*collector.collect(params.getCurrentMean(count));*/
                    collector.collect(newItem);

                   // collector.collect(Gaussian.nextGaussian(0,1));
                }
            }

            @Override
            public void cancel() {

            }
        });

    }



/*    private static DataStream<Double> createStreamFromFile(StreamExecutionEnvironment env) {

        //Single parallelism
        env.setDegreeOfParallelism(1);
        final Random rand = new Random();
        DataStream<Double> values = env.readTextFile("flink-staging/flink-streaming/flink-streaming-examples/src/main/resources/distribution_in.txt")
                .flatMap(new FlatMapFunction<String, Double>() {
                    @Override
                    public void flatMap(String s, Collector<Double> out) throws Exception {
                        String[] args = s.split(" ");
                        double mean = Double.parseDouble(args[0]);
                        double stdev = Double.parseDouble(args[1]);
                        double size = Long.parseLong(args[2]);
                        System.out.println(" mean:" + mean + "  stdev:" + stdev + "  size:" + size);
                        for (int i = 1; i < size; i++) {
                            out.collect(rand.nextGaussian());
                        }
                    }
                });
        return values;
    }*/




/*    private static DataStream<Double> createGaussianEvolvingStream(StreamExecutionEnvironment env, final int length) {
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
    }*/

    private static final class Coin {
        public static boolean flip(int sides) {
            return (Math.random() * sides < 1);
        }
    }

    private static final class Gaussian {
        public static double nextGaussian(double mean, double stDev) { return (new Random().nextGaussian()*stDev + mean); }
    }

}
