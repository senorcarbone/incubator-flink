package org.apache.flink.streaming.examples.nextgen;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.invokable.operator.NextGenCountTriggerPolicy;
import org.apache.flink.streaming.api.invokable.operator.NextGenTriggerPolicy;
import org.apache.flink.streaming.examples.basictopology.BasicTopology.BasicSource;

import java.util.LinkedList;

public class NextGenMultiplePoliciesExample {

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;
	
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);
		
		LinkedList<NextGenTriggerPolicy<String>> policies=new LinkedList<>();
		policies.add(new NextGenCountTriggerPolicy<String>(5));
		policies.add(new NextGenCountTriggerPolicy<String>(8));
		
		//This reduce function does a String concat.
		ReduceFunction<String> reducer=new ReduceFunction<String>() {

			/**
			 * Auto generates version ID
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String reduce(String value1, String value2) throws Exception {
				return value1+"|"+value2;
			}
			
		};
		
		DataStream<Tuple2<String,String[]>> stream = env.addSource(new BasicSource(), SOURCE_PARALLELISM)
				.nextGenBatch(policies,reducer);
				
		stream.print();

		env.execute();
	}
	
}
