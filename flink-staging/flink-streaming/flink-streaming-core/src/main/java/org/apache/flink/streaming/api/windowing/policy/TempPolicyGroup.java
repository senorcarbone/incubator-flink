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

package org.apache.flink.streaming.api.windowing.policy;

import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.extractor.FieldFromTuple;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * A policy group wraps around several deterministic windowing policies which
 * operate on the same field of the input tuples.
 * 
 * @param <DATA>
 *            the type of input tuples handled by this policy group
 */
public class TempPolicyGroup<DATA> extends DeterministicPolicyGroup {
	
	public TempPolicyGroup(DeterministicTriggerPolicy trigger, DeterministicEvictionPolicy eviction, int keyFieldId) {
		super(trigger, eviction, keyFieldId);
	}

	public TempPolicyGroup(DeterministicTriggerPolicy trigger, DeterministicEvictionPolicy eviction, Extractor fieldExtractor) {
		super(trigger, eviction, fieldExtractor);
	}

	public TempPolicyGroup(DeterministicTriggerPolicy trigger, DeterministicEvictionPolicy eviction) {
		super(trigger, eviction);
	}
}
