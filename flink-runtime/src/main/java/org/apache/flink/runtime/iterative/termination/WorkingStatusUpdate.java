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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.AbstractEvent;

import java.io.IOException;

/**
 * An event that is triggered by streaming sources to tell all operators to report their state to the
 * {@link LoopTerminationCoordinator}
 */
public class WorkingStatusUpdate extends AbstractEvent {

	long sequenceNumber;

	String sourceName;

	public WorkingStatusUpdate(){}

	public WorkingStatusUpdate(long seqNum, String sourceName){
		this.sequenceNumber = seqNum;
		this.sourceName = sourceName;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeUTF(sourceName);
		out.writeLong(sequenceNumber);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.sourceName = in.readUTF();
		this.sequenceNumber = in.readLong();
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public String getSourceName() {
		return sourceName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o){return true;}
		if (o == null || getClass() != o.getClass()){return false;}

		WorkingStatusUpdate that = (WorkingStatusUpdate) o;

		if (sequenceNumber != that.sequenceNumber){return false;}
		return sourceName != null ? sourceName.equals(that.sourceName) : that.sourceName == null;

	}

	@Override
	public int hashCode() {
		int result = (int) (sequenceNumber ^ (sequenceNumber >>> 32));
		result = 31 * result + (sourceName != null ? sourceName.hashCode() : 0);
		return result;
	}
}
