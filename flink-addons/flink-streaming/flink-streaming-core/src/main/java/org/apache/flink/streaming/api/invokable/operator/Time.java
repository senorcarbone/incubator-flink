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

package org.apache.flink.streaming.api.invokable.operator;

import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;

public class Time<DATA> implements NextGenWindowHelper<DATA> {

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
	public NextGenEvictionPolicy<DATA> toEvict() {
		return new NextGenTimeEvictionPolicy<DATA>(granularityInMillis(), new DefaultTimeStamp<DATA>());
	}

	@Override
	public NextGenTriggerPolicy<DATA> toTrigger() {
		return new NextGenTimeTriggerPolicy<DATA>(granularityInMillis(), new DefaultTimeStamp<DATA>());
	}

	public static <DATA> Time<DATA> of(int timeVal, TimeUnit granularity) {
		return new Time<DATA>(timeVal, granularity);
	}
	
	private long granularityInMillis(){
		return this.granularity.toMillis(this.timeVal);
	}

}
