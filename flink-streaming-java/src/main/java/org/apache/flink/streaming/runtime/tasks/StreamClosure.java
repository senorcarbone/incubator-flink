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
package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.iterative.termination.LoopTerminationCoordinator;
import org.apache.flink.runtime.iterative.termination.StreamCompleted;
import org.apache.flink.runtime.iterative.termination.WorkingStatusUpdate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * This is a hock class that is started upon finishing a stream source. It starts the communication with
 * {@link LoopTerminationCoordinator} to start the loop termination process.
 */
public class StreamClosure {

	protected static final Logger LOG = LoggerFactory.getLogger(LoopTerminationCoordinator.class);

	private final SourceStreamTask<?,?,?> sourceTask;

	private boolean finalizing;

	CountDownLatch latch  = new CountDownLatch(1);

	public StreamClosure(SourceStreamTask<?,?,?> asourceTask) {
		this.sourceTask = asourceTask;
	}

	protected  void start() throws IOException, InterruptedException {

		finalizing = true;

		// Notifying the coordinator
		notifyCoordinator();
		// awaiting until 'stop' is called
		while(true){
			latch.await();
			if(!finalizing){
				LOG.info("Source Stream Finalizer for source {} is done!",sourceTask.getName());
				break; // we are done
			}else{ // should not happen
				latch  = new CountDownLatch(1);
			}
		}
	}

	public void broadcastStatusEvent(long sequenceNumber) throws IOException, InterruptedException {
		LOG.info("broadcasting a status message event with SEQ"+
				" : {} from task {}",sequenceNumber,sourceTask.getName());
		sourceTask.forwardEvent(new WorkingStatusUpdate(sequenceNumber,sourceTask.getName()));
	}

	public void stop() {
		LOG.info("Stream Closure of task {} will stop",sourceTask.getName());
		this.finalizing = false;
		latch.countDown();
	}

	private void notifyCoordinator(){

		LOG.info("Notifying Loop termination coordinator about stream completion of task {}",sourceTask.getName());

		JobID jobId =  sourceTask.getEnvironment().getJobID();
		JobVertexID vid = sourceTask.getEnvironment().getJobVertexId();
		ExecutionAttemptID executionID = sourceTask.getEnvironment().getExecutionId();
		int subtaskIndex =  sourceTask.getEnvironment().getTaskInfo().getIndexOfThisSubtask();

		sourceTask.tellJobManager(new StreamCompleted(jobId,vid, executionID,subtaskIndex));
	}

}