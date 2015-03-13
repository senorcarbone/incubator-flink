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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by marthavk on 2015-03-11.
 */
public class GraphTriangleSampling implements Serializable{

    Estimator<Edge, Triangle> est = new Estimator<Edge, Triangle>();
    //Triangle<Edge<Long>> triangle = new Triangle<Edge<Long>>();
    //Edge<Long> dummy = new Edge<Long>();


    public GraphTriangleSampling() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setDegreeOfParallelism(1);
        //create edges from file

        DataStream<Edge<Long>> edges = env.readTextFile("flink-staging/flink-streaming/flink-streaming-examples/src/main/resources/random_graph.txt")
                .map(new MapFunction<String, Edge<Long>>() {
                    @Override
                    public Edge<Long> map(String s) throws Exception {
                        // Parse lines from the text file
                        String[] args = s.split(" ");
                        long src = Long.parseLong(args[0]);
                        long trg = Long.parseLong(args[1]);
                        return new Edge<Long>(src, trg);
                    }
                    // sample 1 or 0 triangles
                });

        edges.flatMap(new FlatMapFunction<Edge<Long>, Triangle<Edge<Long>>>() {
                    int counter = 0;

                    @Override
                    public void flatMap(Edge<Long> edge, Collector<Triangle<Edge<Long>>> out) throws Exception {
                        counter++;
                        if (Coin.flip(counter)) {
                            est.reset();
                            est.setR1(edge);
                        } else {
                            if (edge.isAdjacentTo(est.getR1())) {
                                est.incrementC();
                                if (Coin.flip(est.getC())) {
                                    est.setR2(edge);
                                    est.setT(null);
                                } else {
                                    if (est.isTriangle(edge)) {
                                        System.out.println("true");
                                        est.setT(new Triangle(est.getR1(), est.getR2(), edge));
                                        System.out.println("estimator: " + est.toString());
                                        out.collect(est.getTriangle());
                                    }
                                }
                            }
                        }

                    }
                }).print();


/*                .flatMap(new FlatMapFunction<Edge<Long>, Triangle<Edge<Long>>>() {
                    int counter = 0;

                    @Override
                    public void flatMap(Edge<Long> edge, Collector<Triangle<Edge<Long>>> out) throws Exception {
                        counter++;
                        //System.out.println("Edge: " + edge.toString());
                        if (Coin.flip(counter)) {
                            est.reset();
                            est.setR1(edge);
                            //System.out.println("edge inserted as r1. estimator: " + est.toString());
                        } else {
                            if (edge.isAdjacentTo(est.getR1())) {
                                //System.out.println("edge adjacent to " + est.getR1().toString());
                                est.incrementC();
                                if (Coin.flip(est.getC())) {
                                    est.setR2(edge);
                                    est.setT(null);
                                    //System.out.println("edge inserted as r2. estimator: " + est.toString());
                                } else {
                                    //System.out.println("check if edge forms triangle");
                                    if (est.isTriangle(edge)) {
                                        System.out.println("true");
                                        est.setT(new Triangle(est.getR1(), est.getR2(), edge));
                                        System.out.println("estimator: " + est.toString());
                                        out.collect(est.getTriangle());
                                        //out.collect(est.getR1());
                                        //out.collect(est.getR2());
                                        //out.collect(edge);

                                    }
                                    //System.out.println("false");
                                }
                            }
                        }
                    }
                })*/
                //.print();

                /*

                .filter(new FilterFunction<Edge<Long>>() {


                    int counter = 0;

                    @Override
                    public boolean filter(Edge<Long> edge) throws Exception {
                        counter++;
                        System.out.println("Edge: " + edge.toString());
                        if (Coin.flip(counter)) {
                            est.reset();
                            est.setR1(edge);
                            System.out.println("edge inserted as r1. estimator: " + est.toString());
                        } else {
                            if (edge.isAdjacentTo(est.getR1())) {
                                System.out.println("edge adjacent to " + est.getR1().toString());
                                est.incrementC();
                                if (Coin.flip(est.getC())) {
                                    est.setR2(edge);
                                    est.setT(null);
                                    System.out.println("edge inserted as r2. estimator: " + est.toString());
                                } else {
                                    System.out.println("check if edge forms triangle");
                                    if (est.isTriangle(edge)) {
                                        System.out.println("true");
                                        est.setT(new Triangle(est.getR1(), est.getR2(), edge));
                                        System.out.println("estimator: " + est.toString());
                                        //triangle.setFields(est.getR1(), est.getR2(), edge);
                                        return true;
                                    }
                                    System.out.println("false");
                                }
                            }
                        }
                        return false;
                    }
                })
                .addSink(new SinkFunction<Edge<Long>>() {
                    @Override
                    public void invoke(Edge<Long> value) throws Exception {
                        System.out.println(est.getTriangle().toString());
                    }

                    @Override
                    public void cancel() {

                    }
                }); */

        env.execute("Triangle Sampling");
    }

    private static final class Coin {
        public static boolean flip(int sides) {
            return (Math.random() * sides < 1);
        }
    }

    public static void main(String args[]) throws Exception {
        new GraphTriangleSampling();
    }
}
