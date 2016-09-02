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
import org.apache.flink.runtime.iterative.termination.JobTerminationCoordinator;
import org.apache.flink.runtime.jobgraph.JobVertexID;


/**
 * Responder for Loop termination coordinator.
 */
public interface JobTerminationResponder {

	/**
	 * Replies the current working status of the task to  {@link JobTerminationCoordinator}
	 * @param sequenceNumber The id of the update message
	 * @param isIdle The id of the update message
	 * @param jobID Job ID of the running job
	 * @param jobVertexID the task id
	 * @param executionAttemptID Execution attempt ID of the running task
	 * @param subtaskIndex The index of the current executing subtask
	 */
	void replyStatus(int sequenceNumber,boolean isIdle,JobID jobID, JobVertexID jobVertexID, ExecutionAttemptID executionAttemptID,int subtaskIndex);

	/**
	 * Notifies a stream completion to the {@link JobTerminationCoordinator}
	 * @param jobID Job ID of the running job
	 * @param jobVertexID the task id
	 * @param executionAttemptID Execution attempt ID of the running task
	 * @param subtaskIndex The index of the current executing subtask
	 */

	void notifyStreamCompleted(JobID jobID,JobVertexID jobVertexID, ExecutionAttemptID executionAttemptID, int subtaskIndex);


}
