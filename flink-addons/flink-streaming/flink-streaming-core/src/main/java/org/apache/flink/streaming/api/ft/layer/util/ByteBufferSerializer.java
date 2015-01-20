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

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class ByteBufferSerializer extends TypeSerializerSingleton<ByteBuffer> {

	private static final long serialVersionUID = 1L;

	private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);

	public static final ByteBufferSerializer INSTANCE = new ByteBufferSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public ByteBuffer createInstance() {
		return EMPTY;
	}

	@Override
	public ByteBuffer copy(ByteBuffer from) {
		byte[] copy = new byte[from.capacity()];
		System.arraycopy(from.array(), 0, copy, 0, from.capacity());
		return ByteBuffer.wrap(copy);
	}

	@Override
	public ByteBuffer copy(ByteBuffer from, ByteBuffer reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(ByteBuffer record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The record must not be null.");
		}
		final int len = record.limit();
		target.writeInt(len);
		target.write(record.array(), 0, len);
	}

	@Override
	public ByteBuffer deserialize(DataInputView source) throws IOException {
		final int len = source.readInt();
		byte[] result = new byte[len];
		source.readFully(result);
		return ByteBuffer.wrap(result);
	}

	@Override
	public ByteBuffer deserialize(ByteBuffer reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		final int len = source.readInt();
		target.writeInt(len);
		target.write(source, len);
	}

}
