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

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.plugable.SerializationDelegate;

public class SemiDeserializedStreamRecord implements Serializable {

	private static final long serialVersionUID = 1L;

	private ByteBuffer serializedRecord;
	private int hashCode;
	private RecordId id;

	public SemiDeserializedStreamRecord() {
		this.id = new RecordId();
	}

	public SemiDeserializedStreamRecord(ByteBuffer serializedRecord, int hashCode, RecordId id) {
		this.serializedRecord = serializedRecord;
		this.hashCode = hashCode;
		this.id = id;
	}

	public ByteBuffer getSerializedRecord() {
		return serializedRecord;
	}

	public void setSerializedRecord(ByteBuffer serializedRecord) {
		this.serializedRecord = serializedRecord;
	}
	
	public int getRecordLength() {
		return serializedRecord.limit();
	}

	public int getHashCode() {
		return hashCode;
	}

	public void setHashCode(int hashCode) {
		this.hashCode = hashCode;
	}

	public RecordId getId() {
		return id;
	}

	public void setId(RecordId id) {
		this.id = id;
	}
	
	public byte[] getArray(){
		return serializedRecord.array();
	}

	public static SerializationDelegate<SemiDeserializedStreamRecord> createSerializationDelegate() {
		TypeSerializer<SemiDeserializedStreamRecord> serializer = new AsStreamRecordSerializer();
		return new SerializationDelegate<SemiDeserializedStreamRecord>(serializer);
	}

	public String toString() {
		String result = "[" + id.toString() + ",";
		for (int i = 0; i < serializedRecord.limit(); i++) {
			result += " " + serializedRecord.get(i);
		}
		result += "]";
		return result;
	}

}
