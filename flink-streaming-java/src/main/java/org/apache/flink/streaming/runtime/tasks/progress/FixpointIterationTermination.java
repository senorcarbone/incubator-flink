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


package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

public class FixpointIterationTermination implements StreamIterationTermination {
	private Map<List<Long>, Boolean> convergedTracker = new HashMap<>();
	private Set<List<Long>> done = new HashSet<>();

	public boolean terminate(List<Long> timeContext) {
		return done.contains(timeContext);
	}

	public void observeRecord(StreamRecord record) {
		done.remove(record.getProgressContext()); // if this partition is "back alive"
		convergedTracker.put(record.getProgressContext(), false);
	}

	public void observeWatermark(Watermark watermark) {
		if(watermark.getTimestamp() == Long.MAX_VALUE) {
			// clean up
			convergedTracker.remove(watermark.getContext());
			done.remove(watermark.getContext());
		} else {
			Boolean converged = convergedTracker.get(watermark.getContext());
			if(converged != null && converged) {
				done.add(watermark.getContext());
			} else {
				convergedTracker.put(watermark.getContext(), true);
			}
		}
	}
}
