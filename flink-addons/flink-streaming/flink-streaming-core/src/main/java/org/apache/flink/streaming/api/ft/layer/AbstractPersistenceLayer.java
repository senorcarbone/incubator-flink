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

package org.apache.flink.streaming.api.ft.layer;

import java.io.Serializable;

public abstract class AbstractPersistenceLayer<U, V> implements Serializable {
	private static final long serialVersionUID = 1L;

	protected FTLayer ftLayer;
	protected AbstractPersistentStorage<U, V> storage;

	protected AbstractPersistenceLayer(FTLayer ftLayer) {
		this.ftLayer = ftLayer;
		setPersistentStorage();
	}

	public FTLayer getFTLayer() {
		return ftLayer;
	}

	protected abstract void setPersistentStorage();

	public void push(U sourceRecordId, V sourceRecord) {
		storage.push(sourceRecordId, sourceRecord);
	}

	public V get(U sourceRecordId) {
		return storage.get(sourceRecordId);
	}

	public boolean contains(U sourceRecordId) {
		return storage.contains(sourceRecordId);
	}

	public void remove(U sourceRecordId) {
		storage.remove(sourceRecordId);
	}

}