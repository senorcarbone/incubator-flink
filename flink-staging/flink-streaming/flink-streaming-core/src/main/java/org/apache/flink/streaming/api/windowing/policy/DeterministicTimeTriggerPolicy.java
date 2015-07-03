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

import org.apache.flink.streaming.api.windowing.helper.SystemTimestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

public class DeterministicTimeTriggerPolicy<DATA> extends
		TimeTriggerPolicy<DATA> implements DeterministicTriggerPolicy<DATA> {

	long startTime;
	long granularity;

	public DeterministicTimeTriggerPolicy(long granularity,
			TimestampWrapper<DATA> timestampWrapper) {
		super(granularity, timestampWrapper);
		this.startTime = timestampWrapper.getStartTime();
		this.granularity = granularity;
	}

	public DeterministicTimeTriggerPolicy(long granularity) {
		this(granularity, (TimestampWrapper<DATA>) SystemTimestamp.getWrapper());
	}

	@Override
	public double getNextTriggerPosition(double previouseTriggerPosition) {

		if (previouseTriggerPosition < (double) startTime) {
			return startTime + (double) granularity;
		} else {
			return previouseTriggerPosition + (double) granularity;
		}
	}
}
