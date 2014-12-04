package org.apache.flink.streaming.examples.testing;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

public class LongMapKeyIssueStreaming {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment().setBufferTimeout(100);

		DataStream dstream = env.addSource(new SourceFunction<byte[]>() {
			private static final long serialVersionUID = 1959791665107188598L;

			@Override
			public void invoke(Collector<byte[]> c) throws Exception {
				while (true) {
					c.collect(new byte[32]);
				}
			}
		}).map(new MapFunction<byte[], Results>() {

			private static final long serialVersionUID = -3691902950224434973L;

			@Override
			public Results map(byte[] a) throws Exception {
				HashMap<Long, Integer> r = new HashMap<Long, Integer>();
				r.put(System.currentTimeMillis(), a.length);
				return new Results(r);
			}
		})
		// .flatMap(new FlatMapFunction<HashMap<Long, Integer>, String>() {
		//
		// private static final long serialVersionUID = 7966962590308503443L;
		//
		// @Override
		// public void flatMap(HashMap<Long, Integer> value,
		// Collector<String> out) throws Exception {
		// for (Long k : value.keySet()) {
		// out.collect("T : " + k + ", Size : " + value.get(k));
		// }
		// }
		// });
				.addSink(new SinkFunction<Results>() {
					private static final long serialVersionUID = -4094527854733719682L;

					@Override
					public void invoke(Results a) {
						for (Long k : a.getRecords().keySet())
							System.out.println("T : " + k + ", Size : "
									+ a.getRecords().get(k));

					}
				});

		// dstream.print();
		env.execute();
	}

	public static class Results implements Serializable {
		private HashMap<Long, Integer> records;

		public Results() {

		}

		public Results(HashMap<Long, Integer> records) {
			setRecords(records);
		}

		public HashMap<Long, Integer> getRecords() {
			return records;
		}

		public void setRecords(HashMap<Long, Integer> records) {
			this.records = records;
		}
	}
}
