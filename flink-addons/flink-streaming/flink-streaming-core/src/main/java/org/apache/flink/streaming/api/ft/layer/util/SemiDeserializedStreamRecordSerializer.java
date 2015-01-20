/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft.layer.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class SemiDeserializedStreamRecordSerializer extends
		TypeSerializer<SemiDeserializedStreamRecord> {
	private static final long serialVersionUID = 1L;

	protected TypeSerializer<ByteBuffer> typeSerializer;

	public SemiDeserializedStreamRecordSerializer() {
		this.typeSerializer = new ByteBufferSerializer();
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public SemiDeserializedStreamRecord createInstance() {
		try {
			SemiDeserializedStreamRecord t = new SemiDeserializedStreamRecord();
			t.setSerializedRecord(typeSerializer.createInstance());
			return t;
		} catch (Exception e) {
			throw new RuntimeException("Cannot instantiate StreamRecord.", e);
		}
	}

	@Override
	public SemiDeserializedStreamRecord copy(SemiDeserializedStreamRecord from) {
		SemiDeserializedStreamRecord rec = new SemiDeserializedStreamRecord();
		try {
			rec.setSerializedRecord(typeSerializer.copy(from.getSerializedRecord()));
		} catch (Exception e) {
		}
		rec.setHashCode(from.getHashCode());
		rec.setId(from.getId().copy());
		return rec;
	}

	@Override
	public SemiDeserializedStreamRecord copy(SemiDeserializedStreamRecord from,
			SemiDeserializedStreamRecord reuse) {
		try {
			reuse.setSerializedRecord(typeSerializer.copy(from.getSerializedRecord()));
		} catch (Exception e) {
		}
		reuse.setHashCode(from.getHashCode());
		reuse.setId(from.getId().copy());
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(SemiDeserializedStreamRecord record, DataOutputView target)
			throws IOException {
		record.getId().write(target);
		target.writeInt(record.getHashCode());
		typeSerializer.serialize(record.getSerializedRecord(), target);
	}

	@Override
	public SemiDeserializedStreamRecord deserialize(DataInputView source) throws IOException {
		SemiDeserializedStreamRecord record = new SemiDeserializedStreamRecord();
		record.getId().read(source);
		record.setHashCode(Integer.valueOf(source.readInt()));
		record.setSerializedRecord(typeSerializer.deserialize(source));
		return record;
	}

	@Override
	public SemiDeserializedStreamRecord deserialize(SemiDeserializedStreamRecord reuse,
			DataInputView source) throws IOException {
		reuse.getId().read(source);
		reuse.setHashCode(Integer.valueOf(source.readInt()));
		reuse.setSerializedRecord(typeSerializer.deserialize(source));
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// TODO implemented
	}

}
