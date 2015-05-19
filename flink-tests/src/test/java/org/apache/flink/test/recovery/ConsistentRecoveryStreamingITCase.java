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

package org.apache.flink.test.recovery;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;


@SuppressWarnings("serial")
public class ConsistentRecoveryStreamingITCase extends AbstractProcessFailureRecoveryTest {

	private static final int DATA_COUNT = 20000;
	private static final List<Long> PARTIAL_SUMS = Lists.newArrayList(100000000l, 100020000l, 0l);
	private static final long SUM = 200020000;
	private static final int DEFAULT_DOP = 2;

	@Override
	public void testProgram(int jobManagerPort, final File coordinateDir) throws Exception {

		final File tempTestOutput = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH),
				UUID.randomUUID().toString());

		assertTrue("Cannot create directory for temp output", tempTestOutput.mkdirs());

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createRemoteEnvironment("localhost", jobManagerPort);
		env.setParallelism(getParallelism());
		env.getConfig().disableSysoutLogging();
		env.setNumberOfExecutionRetries(1);
		env.enableCheckpointing(200);

		MapFunction<Long, Long> forwarder = new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return value;
			}
		};

		DataStream<Long> src1 = env.addSource(new StatefulSource(coordinateDir, DATA_COUNT))
				.shuffle().map(forwarder).setParallelism(4);
		DataStream<Long> src2 = env.addSource(new StatefulSource(coordinateDir, DATA_COUNT))
				.shuffle().map(forwarder).setParallelism(4);

		src1.connect(src2).groupBy(new Mod(2), new Mod(2)).map(new StatefulTwoInputMapper(coordinateDir, PARTIAL_SUMS))
				.addSink(new StatefulSink(SUM)).setParallelism(1);

		try {
			System.err.println(env.getExecutionPlan());
			env.execute();

		} finally {
			// clean up
			if (tempTestOutput.exists()) {
				FileUtils.deleteDirectory(tempTestOutput);
			}
		}
	}

	@Override
	public int getParallelism() {
		return DEFAULT_DOP;
	}

	public static class StatefulSource extends RichParallelSourceFunction<Long>
			implements Checkpointed<Long> {

		private static final long SLEEP_TIME = 50;

		private final File coordinateDir;
		private final long end;

		private long collected;
		private File proceedFile;
		private boolean checkForProceedFile;
		private boolean hasRecovered;
		private int index;

		public StatefulSource(File coordinateDir, long end) {
			this.coordinateDir = coordinateDir;
			this.end = end;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			index = getRuntimeContext().getIndexOfThisSubtask();
			proceedFile = new File(coordinateDir, PROCEED_MARKER_FILE);
			checkForProceedFile = true;
			assertFalse(hasRecovered && collected == 0d);
			
		}

		private boolean delayInjection() throws InterruptedException {
			// check if the proceed file exists (then we go full speed)
			// if not, we always recheck and sleep
			if (checkForProceedFile) {
				if (proceedFile.exists()) {
					System.err.println("SLOWING DOWN SOURCE");
					checkForProceedFile = false;
				} else {
					// otherwise wait so that we make slow progress
					Thread.sleep(SLEEP_TIME);
				}
			}
			return checkForProceedFile;
		}

		@Override
		public void close() throws Exception {
			System.err.println("CLOSING SRC - COLLECTED: " + (collected - 1) + ", RECOVERED: " + hasRecovered);
			assertTrue(hasRecovered);
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			System.err.println("SRC CHECKPOINTING :" + collected +" for #"+checkpointTimestamp);
			return collected;
		}

		@Override
		public void restoreState(Long state) {
			System.err.println("Restoring src state from "+state);
			hasRecovered = true;
			collected = state;
			assertNotEquals(0d, collected);
		}

		@Override
		public boolean reachedEnd() throws Exception {
			return collected > end; 
		}

		@Override
		public Long next() throws Exception {
			delayInjection();
			if(collected % 10 ==0)System.err.println("SRC "+index+" EMITTED "+collected);
			return collected++;
		}
	}

	public static class Mod implements KeySelector<Long, Object> {

		private final int mod;

		public Mod(int mod)
		{
			this.mod = mod;
		}
		
		@Override
		public Object getKey(Long value) throws Exception {
			return value % mod;
		}
	}

	public static class StatefulTwoInputMapper extends RichCoMapFunction<Long, Long, Long> implements Checkpointed<Long> {
		private final List<Long> partialSums;
		long sum;
		private File coordinateDir;
		private boolean hasRecovered;
		private boolean marked;

		public StatefulTwoInputMapper(File coordinateDir, List<Long> partialSums) {
			this.coordinateDir = coordinateDir;
			this.partialSums = partialSums;
		}

		@Override
		public Long map1(Long value) {
			markFileForTask();
			sum += value;
			return value;
		}

		@Override
		public Long map2(Long value) {
			markFileForTask();
			sum += value;
			return value;
		}

		private void markFileForTask() {
			if (!marked) {
				int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
				try {
					touchFile(new File(coordinateDir, READY_MARKER_FILE_PREFIX + taskIndex));
				} catch (IOException e) {
					e.printStackTrace();
				}
				marked = true;
			}
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return sum;
		}

		@Override
		public void restoreState(Long state) {
			System.err.println("Restoring comap state from "+state);
			hasRecovered = true;
			sum = state;
		}

		@Override
		public void close() throws Exception {
			System.err.println("CLOSING COMAP - SUM: " + sum + ", RECOVERED: " + hasRecovered);
			assertTrue(hasRecovered);
			assertTrue(partialSums.contains(sum));
		}
	}

	public static class StatefulSink extends RichSinkFunction<Long> implements Checkpointed<Long> {

		private final long finalSum;
		long sum;

		public StatefulSink(long finalSum) {
			this.finalSum = finalSum;
		}

		@Override
		public void invoke(Long value) throws Exception {
			sum += value;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return sum;
		}

		@Override
		public void restoreState(Long state) {
			System.err.println("Restoring sink state from "+state);
			sum = state;
		}

		@Override
		public void close() throws Exception {
			assertEquals(finalSum, sum);
		}
	}


}
