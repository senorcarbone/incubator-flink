/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.paper;

import java.io.Serializable;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PaperExperiment implements Serializable {

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			throw new IllegalArgumentException();
		}
		Long checkpointInterval = Long.parseLong(args[0]);
		Long numTuplesPerSource = Long.parseLong(args[1]);
		Long numKeys = Long.parseLong(args[2]);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (checkpointInterval > 0) {
			env.getStreamGraph().setCheckpointingEnabled(true);
			env.getStreamGraph().setCheckpointingInterval(checkpointInterval);
		}

		DataStream<Long> groupSum = env.addSource(new StatefulSource(numTuplesPerSource))
				.groupBy(new ModKey(numKeys)).sum(0);

		groupSum.distribute().map(new StatefulMap()).addSink(new NoOpSink());
		groupSum.distribute().map(new StatefulMap()).addSink(new NoOpSink());

		JobExecutionResult res = env.execute();
		System.err.println(res.getNetRuntime());
	}

	public static class ModKey implements KeySelector<Long, Long> {

		Long mod;

		public ModKey(Long mod) {
			this.mod = mod;
		}

		@Override
		public Long getKey(Long value) throws Exception {
			return value % mod;
		}

	}

	public static class StatefulMap extends RichMapFunction<Long, Long> {

		OperatorState<Long> state;

		@Override
		public Long map(Long value) throws Exception {
			state.update(value);
			return value;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void open(Configuration conf) {
			StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

			if (context.containsState("mapState")) {
				state = (OperatorState<Long>) context.getState("mapState");
			} else {
				state = new OperatorState<Long>(0L);
				context.registerState("mapState", state);
			}
		}

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
				collector.collect(i);
				offset.update(i);
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

	public static class NoOpSink implements SinkFunction<Long> {

		@Override
		public void invoke(Long value) throws Exception {

		}

	}
}
