package org.apache.flink.streaming.api;

import java.io.Serializable;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PaperExperiment implements Serializable {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);
		env.getStreamGraph().setCheckpointingEnabled(true);
		env.getStreamGraph().setCheckpointingInterval(50);

		env.addSource(new StatefulSource(1000000000L)).groupBy(new KeySelector<Long, Long>() {

			@Override
			public Long getKey(Long value) throws Exception {
				return value % 40;
			}

		}).sum(0).addSink(new SinkFunction<Long>() {

			@Override
			public void invoke(Long value) throws Exception {

			}
		});

		JobExecutionResult res = env.execute();
		System.out.println(res.getNetRuntime());
	}

	public static class StatefulSource extends RichParallelSourceFunction<Long> {

		OperatorState<Long> offset;
		private Long max;
		private int taskIndex;

		public StatefulSource(Long max) {
			this.max = max;
		}

		@Override
		public void run(Collector<Long> collector) throws Exception {

			for (Long i = offset.getState(); i < max; i++) {
				if (i % 4 == taskIndex) {
					collector.collect(i);
				}
			}
		}

		@Override
		public void cancel() {

		}

		@SuppressWarnings("unchecked")
		@Override
		public void open(Configuration conf) {
			taskIndex = getRuntimeContext().getIndexOfThisSubtask();
			StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

			if (context.containsState("offset")) {
				offset = (OperatorState<Long>) context.getState("offset");
			} else {
				offset = new OperatorState<Long>(0L);
				context.registerState("offset", offset);
			}
		}

	}
}
