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

package org.apache.flink.streaming.api.ft.layer.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.plugable.SerializationDelegate;

public class RecordId implements IOReadableWritable, Serializable, Comparable<RecordId> {

	private static final long serialVersionUID = 1L;

	private static Random random = new Random();

	private long recordId;
	private long sourceRecordId;

	public RecordId() {
	}

	public RecordId(long recordId, long sourceRecordId) {
		this.recordId = recordId;
		this.sourceRecordId = sourceRecordId;
	}

	public static RecordId newSourceXorMessage(long offset) {
		RecordId sourceXorMessage = new RecordId();
		sourceXorMessage.recordId = offset;
		sourceXorMessage.sourceRecordId = random.nextLong();
		return sourceXorMessage;
	}

	public static RecordId newSourceRecordId() {
		RecordId sourceXorMessage = new RecordId();
		long rnd = random.nextLong();
		sourceXorMessage.recordId = rnd;
		sourceXorMessage.sourceRecordId = rnd;
		return sourceXorMessage;
	}

	public static RecordId newRecordId(long sourceRecordId) {
		RecordId sourceXorMessage = new RecordId();
		sourceXorMessage.recordId = random.nextLong();
		sourceXorMessage.sourceRecordId = sourceRecordId;
		return sourceXorMessage;
	}

	public void setRecordIdToSourceRecordId() {
		recordId = sourceRecordId;
	}

	public long getRecordId() {
		return recordId;
	}

	public long getSourceRecordId() {
		return sourceRecordId;
	}

	public static SerializationDelegate<RecordId> createSerializationDelegate() {
		TypeInformation<RecordId> typeInfo = TypeExtractor.getForObject(new RecordId());
		return new SerializationDelegate<RecordId>(typeInfo.createSerializer());
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(recordId);
		out.writeLong(sourceRecordId);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		recordId = in.readLong();
		sourceRecordId = in.readLong();
	}

	public RecordId copy() {
		return new RecordId(recordId, sourceRecordId);
	}

	@Override
	public String toString() {
		return "s:" + Long.toHexString(sourceRecordId) + "\txor\t" + Long.toHexString(recordId);
	}

	@Override
	public boolean equals(Object otherId) {
		if (otherId == null) {
			return false;
		} else if (this.getClass() == otherId.getClass()) {
			return this.recordId == ((RecordId) otherId).getRecordId()
					&& this.sourceRecordId == ((RecordId) otherId).getSourceRecordId();
		}
		return false;
	}

	@Override
	public int compareTo(RecordId other) {
		if (sourceRecordId < other.sourceRecordId
				|| (sourceRecordId == other.sourceRecordId && recordId < other.recordId)) {
			return -1;
		} else if (this.equals(other)) {
			return 0;
		} else {
			return 1;
		}
	}
	
	@Override
	public int hashCode(){
		return (int) recordId;
	}

}
