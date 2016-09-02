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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.iterative.termination.StreamCompleted;
import org.apache.flink.runtime.iterative.termination.TaskWorkingStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

/**
 * Implementation using {@link ActorGateway} to forward the messages.
 */
public class ActorGatewayJobTerminationResponder implements JobTerminationResponder {

	private final ActorGateway actorGateway;

	public ActorGatewayJobTerminationResponder(ActorGateway actorGateway) {
		this.actorGateway = Preconditions.checkNotNull(actorGateway);
	}

	@Override
	public void replyStatus(int sequenceNumber,boolean isIdle,JobID jobId, JobVertexID jobVertexID, ExecutionAttemptID executionAttemptID,int subtaskIndex) {
		TaskWorkingStatus msg =  new TaskWorkingStatus(sequenceNumber,isIdle,jobId,jobVertexID,executionAttemptID,subtaskIndex);
		actorGateway.tell(msg);
	}
	@Override
	public void notifyStreamCompleted(JobID jobID,JobVertexID jobVertexID, ExecutionAttemptID executionAttemptID, int subtaskIndex){
		StreamCompleted msg = new StreamCompleted(jobID,jobVertexID, executionAttemptID,subtaskIndex);
		actorGateway.tell(msg);
	}
}
