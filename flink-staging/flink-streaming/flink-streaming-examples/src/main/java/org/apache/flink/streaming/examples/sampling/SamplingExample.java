package org.apache.flink.streaming.examples.sampling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;

import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Random;
/**
 * Created by marthavk on 2015-03-06.
 */
public class SamplingExample {

    public static void main (String[] args) throws Exception {


        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setDegreeOfParallelism(1);

        // get text data stream
        //DataStream<String> text = getTextDataStream(env);

        // generate stream
        //DataStream<Long> dataStream = getRandomSequence(env);
        DataStream<Double> gaussianStream = createGaussianStream(env, 10000);

        int windowSize = 100;
        int reservoirSize = 20;
        //reservoirSampling(dataStream,reservoirSize);
        //windowSampling2(gaussianStream, reservoirSize, windowSize);
        modelApproximation(gaussianStream, reservoirSize);
        //mdSampling(createMultidimensionalStream(dataStream), env, reservoirSize);

        env.execute("Sampling Example");

    }


    /**
     * Performs standard reservoir sampling. Each item in the stream has 1/rSize final probability
     * to be included in the sample.
     * As the stream evolves, each new element is picked by probability equal to  count/rSize where
     * count is its position in the stream.
     * That means if the reservoir size hasn't reached rSize, each element will be definitely picked.
     * If an item is picked and the reservoir is full then it replaces an existing element uniformly at
     * random.
     * @param dataStream
     * @param rSize
     */
    public static void reservoirSampling(DataStream<Long> dataStream, final Integer rSize) {

        dataStream.map(new MapFunction<Long, Reservoir<Long>>() {
            Reservoir<Long> r = new Reservoir<Long>(rSize);
            int count = 0;

            @Override
            public Reservoir<Long> map(Long aLong) throws Exception {
                count++;
                if (Coin.flip(count / rSize)) {
                    r.insertElement(aLong);
                }
                return r;
            }

        });
    }

    /**
     * Windows the dataStream into windows of size wSize and constructs different reservoirs
     * for each window. Then merges the reservoirs into one big reservoir.
     * @param dataStream
     * @param rSize
     * @param wSize
     */
    public static void windowSampling(DataStream<Long> dataStream, final Integer rSize, final Integer wSize) {
        dataStream
                .window(Count.of(wSize)).mapWindow(new WindowMapFunction<Long, Reservoir<Long>>() {
                    @Override
                    public void mapWindow(Iterable<Long> values, Collector<Reservoir<Long>> out) throws Exception {
                        Reservoir<Long> r = new Reservoir<Long>(rSize);
                        int count = 0;
                        for (Long v : values) {
                            count++;
                            if (Coin.flip(count / rSize)) {
                                r.insertElement(v);
                            }
                        }
                        out.collect(r);
                    }
                })
                .flatten()
                .reduce(new ReduceFunction<Reservoir<Long>>() {
                    @Override
                    public Reservoir<Long> reduce(Reservoir<Long> value1, Reservoir<Long> value2) throws Exception {
                        return Reservoir.merge(value1, value2);
                        //return null;
                    }
                }).print();
    }


    public static void windowSampling2(DataStream<Double> dataStream, final Integer rSize, final Integer wSize) {
        WindowedDataStream<Reservoir<Double>> w = dataStream.window(Count.of(wSize))
                .mapWindow(new WindowMapFunction<Double, Reservoir<Double>>() {
                    @Override
                    public void mapWindow(Iterable<Double> values, Collector<Reservoir<Double>> out) throws Exception {
                        Reservoir<Double> r = new Reservoir<Double>(rSize);
                        int count = 0;
                        for (Double v : values) {
                            count++;
                            if (Coin.flip(count / rSize)) {
                                r.insertElement(v);
                            }
                        }
                        out.collect(r);
                    }
                });
    }

    public static void modelApproximation1(DataStream<Double> gaussianStream, final int rSize) {


        DataStream<Tuple3<Reservoir<Double>, Nmodel, Nmodel>> transformedStream = gaussianStream

                .map(new MapFunction<Double,  Tuple3 < Reservoir < Double >, Nmodel, Nmodel >> () {
                    Nmodel<Double> generalModel = new Nmodel<Double>();
                    Nmodel<Double> currentModel = new Nmodel<Double>();
                    Reservoir<Double> r = new Reservoir<Double>(rSize);
                    int count = 0;

                    @Override
                    public Tuple3 <Reservoir<Double>, Nmodel, Nmodel > map(Double aLong) throws Exception {
                        //update model from all elements
                        count++;
                        generalModel.updateModel(aLong);
                        if (Coin.flip(count / rSize)) {
                            r.insertElement(aLong);
                            currentModel.inferModel(r);
                            //calculate model parameters from reservoir
                        }
                        return new Tuple3<Reservoir<Double>, Nmodel, Nmodel>(r, generalModel, currentModel);
                    }

                }).print();
    }

    public static void modelApproximation(DataStream<Double> gaussianStream, final int rSize) {
        DataStream<Tuple2<Reservoir<Double>, Nmodel<Double>>> transformedStream = gaussianStream
                .map(new MapFunction<Double, Tuple2<Reservoir<Double>, Nmodel<Double>>>() {
                    Nmodel<Double> currentModel = new Nmodel<Double>();
                    Reservoir<Double> r = new Reservoir<Double>(rSize);
                    int count = 0;
                    @Override
                    public Tuple2<Reservoir<Double>, Nmodel<Double>> map(Double value) throws Exception {
                        //update model from all elements
                        count++;
                        if (Coin.flip(count / rSize)) {
                            r.insertElement(value);
                            currentModel.inferModel(r);
                            //calculate model parameters from reservoir
                        }
                        return new Tuple2<Reservoir<Double>, Nmodel<Double>>(r,currentModel);
                    }
                });
    }

    public static void mdSampling(DataStream<Tuple3<Integer, Long, Long>> dataStream, StreamExecutionEnvironment env, final int rSize) {
        env.setDegreeOfParallelism(Runtime.getRuntime().availableProcessors());
        //env.setDegreeOfParallelism(env.getDegreeOfParallelism());
    }

/*    private static DataStream<String> getTextDataStream(StreamExecutionEnvironment env) {
        return env.fromElements(WordCountData.WORDS);
    }*/

    private static DataStream<Long> getRandomSequence(StreamExecutionEnvironment env) {
        return  env.generateSequence(1, 100);
    }


    private static DataStream<Tuple3<Integer, Long, Long>> createMultidimensionalStream(DataStream<Long> dataStream) {
        return dataStream.map(new MapFunction<Long, Tuple3<Integer, Long, Long>>() {

            /** Generate a Tuple3 stream with T0 some Integer between 0 and 3
             * (1, 2, 3 or 4) declaring the substream they belong and two random
             * Long assigned for T1 and T2
             */

            @Override
            public Tuple3<Integer, Long, Long> map(Long value) throws Exception {
                Long rand1 = new Random().nextLong()%10;
                Long rand2 = new Random().nextLong()%5;
                Integer subs = new Random().nextInt(4)+1;
                return new Tuple3<Integer, Long, Long>(subs, rand1, rand2);
            }
        });
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

    private static final class Coin {
        public static boolean flip(int sides) {
            return (Math.random() * sides < 1);
        }
    }

    public static class Nmodel<T> implements Serializable {
        /** Normal Distribution Model **/
        private double mean;
        private double var;
        private int size;

        public Nmodel() {

        }

        public void inferModel(Iterable<T> sample){
            //calculate the mean
            size = 0;
            for (T value : sample) {
                size ++;
                mean += (Double)value;
            }
            mean = mean / size;

            //calculate the variance
            for (T value : sample) {
                var += Math.pow(((Double)value - mean),2);
            }
            var = var/size;
        }


        public void updateModel(T dataPoint) {
            double new_mean = (size*mean + (Double)dataPoint)/(size+1);
            var = (size*(var+Math.pow(mean,2))+Math.pow((Double)dataPoint,2))/(size+1) - Math.pow(new_mean,2);
            mean = new_mean;
            size ++;
        }
        

        @Override
        public String toString() {
            return "" + "["+mean+","+var+"]";
        }
    }

}
