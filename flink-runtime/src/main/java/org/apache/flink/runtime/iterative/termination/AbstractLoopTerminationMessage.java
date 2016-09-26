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
 * The base class of all loop(iteration) termination  messages.
 */
public abstract class AbstractLoopTerminationMessage implements java.io.Serializable {

	private static final long serialVersionUID = 186780414819428178L;

	private final JobID job;

	private  ExecutionAttemptID taskExecutionId;

	private final JobVertexID taskVertexID;

	public AbstractLoopTerminationMessage(JobID job, JobVertexID taskVertexID, ExecutionAttemptID taskExecutionId) {
		if (job == null ) {
			throw new NullPointerException();
		}
		this.job = job;
		this.taskExecutionId = taskExecutionId;
		this.taskVertexID = taskVertexID;

	}

	protected AbstractLoopTerminationMessage(JobID job,  JobVertexID taskVertexID) {
		this(job,taskVertexID,null);
	}

	// --------------------------------------------------------------------------------------------
	
	public JobID getJobID() {
		return job;
	}

	public ExecutionAttemptID getTaskExecutionId() {
		return taskExecutionId;
	}

	public JobVertexID getTaskVertexID() {
		return taskVertexID;
	}

	// --------------------------------------------------------------------------------------------
	@Override
	public boolean equals(Object o) {
		if (this == o){ return true;}
		if (o == null || getClass() != o.getClass()){return false;}

		AbstractLoopTerminationMessage that = (AbstractLoopTerminationMessage) o;

		if (job != null ? !job.equals(that.job) : that.job != null){return false;}
		if (taskExecutionId != null ? !taskExecutionId.equals(that.taskExecutionId) : that.taskExecutionId != null) {
			return false;
		}
		return taskVertexID != null ? taskVertexID.equals(that.taskVertexID) : that.taskVertexID == null;

	}

	@Override
	public int hashCode() {
		int result = job != null ? job.hashCode() : 0;
		result = 31 * result + (taskExecutionId != null ? taskExecutionId.hashCode() : 0);
		result = 31 * result + (taskVertexID != null ? taskVertexID.hashCode() : 0);
		return result;
	}
	@Override
	public String toString() {
		return "AbstractLoopTerminationMessage{" +
				"job=" + job +
				", taskExecutionId=" + taskExecutionId +
				'}';
	}
}
