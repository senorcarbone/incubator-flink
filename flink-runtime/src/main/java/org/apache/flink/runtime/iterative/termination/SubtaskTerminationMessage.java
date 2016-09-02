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
package org.apache.flink.runtime.iterative.termination;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * An extension to {@link JobTerminationMessage} message used for sending messages to specific sub-tasks .
 */
public class SubtaskTerminationMessage extends JobTerminationMessage {

	private final int subtaskIndex;
	public SubtaskTerminationMessage(JobID job, JobVertexID taskVertexID, ExecutionAttemptID taskExecutionId, int subtaskIndex) {
		super(job,taskVertexID,taskExecutionId);
		this.subtaskIndex=subtaskIndex;
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o){return true;}
		if (o == null || getClass() != o.getClass()){return false;}
		if (!super.equals(o)){return false;}

		SubtaskTerminationMessage that = (SubtaskTerminationMessage) o;

		return subtaskIndex == that.subtaskIndex;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + subtaskIndex;
		return result;
	}
}
