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

package org.apache.flink.streaming.api.windowing.helper;

import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

public class Time<DATA> implements WindowingHelper<DATA> {

	private int timeVal;
	private TimeUnit granularity;

	public Time(int timeVal, TimeUnit granularity) {
		this.timeVal = timeVal;
		this.granularity = granularity;
	}

	public Time(int timeVal) {
		this(timeVal, TimeUnit.SECONDS);
	}

	@Override
	public EvictionPolicy<DATA> toEvict() {
		return new TimeEvictionPolicy<DATA>(granularityInMillis(),
				new DefaultTimeStamp<DATA>());
	}

	@Override
	public TriggerPolicy<DATA> toTrigger() {
		return new TimeTriggerPolicy<DATA>(granularityInMillis(),
				new DefaultTimeStamp<DATA>());
	}

	public static <DATA> Time<DATA> of(int timeVal, TimeUnit granularity) {
		return new Time<DATA>(timeVal, granularity);
	}

	private long granularityInMillis() {
		return this.granularity.toMillis(this.timeVal);
	}

}
