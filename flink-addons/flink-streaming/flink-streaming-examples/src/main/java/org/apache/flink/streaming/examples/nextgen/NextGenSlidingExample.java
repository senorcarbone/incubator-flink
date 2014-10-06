package org.apache.flink.streaming.examples.nextgen;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.operator.NextGenCountEvictionPolicy;
import org.apache.flink.streaming.api.invokable.operator.NextGenCountTriggerPolicy;
import org.apache.flink.streaming.api.invokable.operator.NextGenEvictionPolicy;
import org.apache.flink.streaming.api.invokable.operator.NextGenTriggerPolicy;
import org.apache.flink.util.Collector;

public class NextGenSlidingExample {

    private static final int PARALLELISM = 1;
    private static final int SOURCE_PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironment(PARALLELISM);

        /* SIMPLE-EXAMPLE:
		 * Use this to always keep the newest 10 elements in the buffer
		 * Resulting windows will have an overlap of 5 elements
		 */
		//NextGenTriggerPolicy<String> triggerPolicy=new NextGenCountTriggerPolicy<String>(5);
		//NextGenEvictionPolicy<String> evictionPolicy=new NextGenCountEvictionPolicy<String>(10);
		
		/* ADVANCED-EXAMPLE:
		 * Use this to have the last element of the last window
		 * as first element of the next window while the window size is always 5
		 */
		NextGenTriggerPolicy<String> triggerPolicy=new NextGenCountTriggerPolicy<String>(4,-1);
		NextGenEvictionPolicy<String> evictionPolicy=new NextGenCountEvictionPolicy<String>(5,4);
        
        //This reduce function does a String concat.
        ReduceFunction<String> reduceFunction = new ReduceFunction<String>() {

            /**
             * default version ID
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String reduce(String value1, String value2) throws Exception {
                return value1 + "|" + value2;
            }

        };

        DataStream<Tuple2<String, String[]>> stream = env.addSource(new CountingSource(), SOURCE_PARALLELISM)
                .nextGenWindow(triggerPolicy,evictionPolicy,reduceFunction);

        stream.print();
        
        env.execute();
    }
    
    @SuppressWarnings("serial")
	private static class CountingSource implements SourceFunction<String>{

    	private int counter=0;
    	
		@Override
		public void invoke(Collector<String> collector) throws Exception {
			// continuous emit
			while (true) {
				if (counter>9999)counter=0;
				collector.collect("V"+counter++);
			}
		}
    	
    }
}
