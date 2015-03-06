package org.apache.flink.streaming.examples.sampling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;
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

        // get text data stream
        //DataStream<String> text = getTextDataStream(env);

        // generate stream
        DataStream<Long> dataStream = getRandomSequence(env);


        int windowSize = 20;
        int reservoirSize = 5;
        //reservoirSampling(dataStream,reservoirSize);
        //windowSampling(dataStream, windowSize, reservoirSize);
        //mdSampling();



        env.execute("Sampling Example");

    }



    private static DataStream<Long> getRandomSequence(StreamExecutionEnvironment env) {
        return  env.generateSequence(1, 100);
    }


/*    private static DataStream<Tuple3<Integer, Long, Long>> createMultidimensionalStream(DataStream<Long> dataStream) {
        return dataStream.map(new MapFunction<Long, Tuple3<Integer, Long, Long>>() {


            *//** Generate a Tuple3 stream with T0 some Integer between 0 and 3
     * (0, 1, 2 or 3) declaring the substream they belong and two random
     * Long assigned for T1 and T2
     *//*


            @Override
            public Tuple3<Integer, Long, Long> map(Long value) throws Exception {
                Long rand1 = new Random().nextLong();
                Long rand2 = new Random().nextLong();
                Integer subs = new Random().nextInt(4);
                return new Tuple3<Integer, Long, Long>(subs, rand1, rand2);
            }
        });
    }
    */


    public static void reservoirSampling(DataStream<Long> dataStream, final Integer size) {
        dataStream.map(new MapFunction<Long, Reservoir<Long>>() {
            Reservoir<Long> r = new Reservoir<Long>(size);
            int count = 0;

            @Override
            public Reservoir<Long> map(Long aLong) throws Exception {
                count++;
                if (Coin.flip(count / size)) {
                    r.insertElement(aLong);
                }
                return r;
            }

        });
    }



    public static void windowSampling(DataStream<Long> dataStream, final Integer rSize, final Integer wSize) {
        dataStream.window(Count.of(wSize)).mapWindow(new WindowMapFunction<Long, Object>() {
            @Override
            public void mapWindow(Iterable<Long> values, Collector<Object> out) throws Exception {
                int reservoirSize = 5;
                Reservoir r = new Reservoir(reservoirSize);
                int count =0;
                for (Long v : values) {
                    count ++;
                    Long rand = Math.abs(new Random().nextLong())%count;
                    if (!r.isFull() || rand < reservoirSize) {
                        r.insertElement(v);
                    }
                }
                //r.print();
                //out.close();
                //out.collect(r);
            }
        }).getDiscretizedStream().print();
    }

    public static void mdSampling(DataStream<Tuple3> dataStream) {
        /*dataStream.setConnectionType(new StreamPartitioner<Tuple3>() {
            @Override
            public int[] selectChannels(SerializationDelegate<StreamRecord<Tuple3>> record, int numChannels) {
                return new int[0];
            }
        });*/
    }

//    static void oneDStream(DataStreamSource<Long> src) {
//        src.map(new MapFunction<Long, Tuple2<Integer, Long>>() {
//
//    * Generate a Tuple2 stream with value1 some integer between 0 and 3
//             * (0, 1, 2 or 3) declaring the substream they belong and a random
//             * Long assigned to each Tuple2
//
//
//            @Override
//            public Tuple2<Integer, Long> map(Long value) throws Exception {
//                Long rand = new Random().nextLong();
//                Integer subs = new Random().nextInt(4);
//                return new Tuple2<Integer, Long>(subs, rand);
//            }
//        })
//            .window(Count.of(20))
//            .mapWindow(new WindowMapFunction<Tuple2<Integer, Long>, Object>() {
//                    @Override
//                    public void mapWindow(Iterable<Tuple2<Integer, Long>> values, Collector<Object> out) throws Exception {
//                        int reservoirSize = 5;
//                        Reservoir r = new Reservoir(reservoirSize);
//                        int count = 0;
//                        for (Tuple2<Integer, Long> v : values) {
//                            count++;
//                            Long rand = Math.abs(new Random().nextLong()) % count;
//                            if (!r.isFull() || rand < reservoirSize) {
//                                r.insertElement(v);
//                            }
//                        }
//                        r.print();
//                        //out.close();
//                        //out.collect(r);
//                    }
//            });
//
//    }

/*    private static DataStream<String> getTextDataStream(StreamExecutionEnvironment env) {
        return env.fromElements(WordCountData.WORDS);
    }*/

    private static final class Coin {
        public static boolean flip(int sides) {
            return (Math.random() * sides < 1);
        }
    }

}
