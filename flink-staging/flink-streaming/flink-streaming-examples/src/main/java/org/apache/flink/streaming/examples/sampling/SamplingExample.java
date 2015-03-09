package org.apache.flink.streaming.examples.sampling;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

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
        DataStream<Long> dataStream = getRandomSequence(env);

        int windowSize = 100;
        int reservoirSize = 15;
        //reservoirSampling(dataStream,reservoirSize);
        windowSampling2(dataStream, reservoirSize, windowSize);
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

    public static void windowSampling2(DataStream<Long> dataStream, final Integer rSize, final Integer wSize) {
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
        }).flatten().print();
    }

    public static void mdSampling(DataStream<Tuple3<Integer, Long, Long>> dataStream, StreamExecutionEnvironment env, final Integer rSize) {
        env.setDegreeOfParallelism(Runtime.getRuntime().availableProcessors());
        //env.setDegreeOfParallelism(env.getDegreeOfParallelism());


    }

    public static



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

    private static final class Coin {
        public static boolean flip(int sides) {
            return (Math.random() * sides < 1);
        }
    }

}
