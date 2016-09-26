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
 * A message sent from the coordinator {@link LoopTerminationCoordinator} to heads and sources to start
 * broadcasting a {@link WorkingStatusUpdate}.
 * */
public class BroadcastStatusUpdateEvent extends AbstractLoopTerminationMessage {

	private final long sequenceNumber;
	protected BroadcastStatusUpdateEvent(long sequenceNumber, JobID job, JobVertexID taskVertexID, ExecutionAttemptID taskExecutionId) {
		super(job,taskVertexID,taskExecutionId);
		this.sequenceNumber = sequenceNumber;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o){ return true;}
		if (o == null || getClass() != o.getClass()){ return false;}
		if (!super.equals(o)){ return false;}

		BroadcastStatusUpdateEvent that = (BroadcastStatusUpdateEvent) o;

		return sequenceNumber == that.sequenceNumber;

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
		return result;
	}

}
