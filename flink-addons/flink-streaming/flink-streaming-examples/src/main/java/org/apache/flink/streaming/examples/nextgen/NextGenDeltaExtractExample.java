package org.apache.flink.streaming.examples.nextgen;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.operator.Count;
import org.apache.flink.streaming.api.invokable.operator.Delta;
import org.apache.flink.streaming.api.invokable.operator.FieldsFromTuple;
import org.apache.flink.streaming.util.nextGenDeltaFunction.EuclideanDistance;
import org.apache.flink.util.Collector;

public class NextGenDeltaExtractExample {

	private static final int PARALLELISM = 1;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		ReduceFunction<Tuple3<Double, Double, String>> concatStrings = new ReduceFunction<Tuple3<Double, Double, String>>() {
			@Override
			public Tuple3 reduce(Tuple3 value1, Tuple3 value2) throws Exception {
				return new Tuple3(value1.f0, value2.f1, value1.f2 + "|" + value2.f2);
			}
		};

		DataStream dstream = env
				.addSource(new CountingSource())
				.window(Delta.of(new EuclideanDistance(new FieldsFromTuple(0, 1)), new Tuple3(0d, 0d, "foo"), 1))
				.every(Count.of(2))
				.reduce(concatStrings);

		dstream.print();
		env.execute();

	}

	private static class CountingSource implements SourceFunction<Tuple3<Double, Double, String>> {

		private int counter = 0;

		@Override
		public void invoke(Collector<Tuple3<Double, Double, String>> collector) throws Exception {
			while (true) {
				if (counter > 9999) counter = 0;
				collector.collect(new Tuple3<Double, Double, String>((double) counter, (double) counter + 1, "V" + counter++));
			}
		}
	}

}
