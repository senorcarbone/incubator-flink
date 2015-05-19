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

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Test the recovery of a simple batch program in the case of TaskManager process failure.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class ProcessFailureBatchRecoveryITCase extends AbstractProcessFailureRecoveryTest {
	private static final int DEFAULT_DOP = 4;

	// --------------------------------------------------------------------------------------------
	//  Parametrization (run pipelined and batch)
	// --------------------------------------------------------------------------------------------

	private final ExecutionMode executionMode;

	public ProcessFailureBatchRecoveryITCase(ExecutionMode executionMode) {
		this.executionMode = executionMode;
	}

	@Parameterized.Parameters
	public static Collection<Object[]> executionMode() {
		return Arrays.asList(new Object[][]{
				{ExecutionMode.PIPELINED},
				{ExecutionMode.BATCH}});
	}

	// --------------------------------------------------------------------------------------------
	//  Test the program
	// --------------------------------------------------------------------------------------------

	@Override
	public void testProgram(int jobManagerPort, final File coordinateDir) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", jobManagerPort);
		env.setParallelism(getParallelism());
		env.setNumberOfExecutionRetries(1);
		env.getConfig().setExecutionMode(executionMode);
		env.getConfig().disableSysoutLogging();

		final long NUM_ELEMENTS = 100000L;
		final DataSet<Long> result = env.generateSequence(1, NUM_ELEMENTS)

				// make sure every mapper is involved (no one is skipped because of lazy split assignment)
				.rebalance()
						// the majority of the behavior is in the MapFunction
				.map(new RichMapFunction<Long, Long>() {

					private final File proceedFile = new File(coordinateDir, PROCEED_MARKER_FILE);

					private boolean markerCreated = false;
					private boolean checkForProceedFile = true;

					@Override
					public Long map(Long value) throws Exception {
						if (!markerCreated) {
							int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
							touchFile(new File(coordinateDir, READY_MARKER_FILE_PREFIX + taskIndex));
							markerCreated = true;
						}

						// check if the proceed file exists
						if (checkForProceedFile) {
							if (proceedFile.exists()) {
								checkForProceedFile = false;
							} else {
								// otherwise wait so that we make slow progress
								Thread.sleep(100);
							}
						}
						return value;
					}
				})
				.reduce(new ReduceFunction<Long>() {
					@Override
					public Long reduce(Long value1, Long value2) {
						return value1 + value2;
					}
				});

		long sum = result.collect().get(0);
		assertEquals(NUM_ELEMENTS * (NUM_ELEMENTS + 1L) / 2L, sum);
	}

	@Override
	public int getParallelism() {
		return DEFAULT_DOP;
	}
}
