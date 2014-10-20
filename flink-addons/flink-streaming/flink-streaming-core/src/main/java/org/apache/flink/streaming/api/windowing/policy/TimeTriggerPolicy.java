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

import org.apache.flink.streaming.api.invokable.util.TimeStamp;

/**
 * This trigger policy triggers with regard to the time. The is measured using a
 * given {@link TimeStamp} implementation. A point in time is always represented
 * as long. Therefore, parameters such as granularity and delay can be set as
 * long value as well.
 * 
 * @param <DATA>
 *            The type of the incoming data points which are processed by this
 *            policy.
 */
public class TimeTriggerPolicy<DATA> implements TriggerPolicy<DATA> {

	/**
	 * auto generated version id
	 */
	private static final long serialVersionUID = -5122753802440196719L;

	private long startTime;
	private long granularity;
	private TimeStamp<DATA> timestamp;

	/**
	 * This trigger policy triggers with regard to the time. The is measured
	 * using a given {@link TimeStamp} implementation. A point in time is always
	 * represented as long. Therefore, parameters such as granularity can be set
	 * as long value as well. If this value for the granularity is set to 2 for
	 * example, the policy will trigger at every second point in time.
	 * 
	 * @param granularity
	 *            The granularity of the trigger. If this value is set to 2 the
	 *            policy will trigger at every second time point
	 * @param timestamp
	 *            The {@link TimeStamp} to measure the time with. This can be
	 *            either user defined of provided by the API.
	 */
	public TimeTriggerPolicy(long granularity, TimeStamp<DATA> timestamp) {
		this(granularity, timestamp, 0);
	}

	/**
	 * This is mostly the same as
	 * {@link TimeTriggerPolicy#TimeTriggerPolicy(long, TimeStamp)}. In addition
	 * to granularity and timestamp a delay can be specified for the first
	 * trigger. If the start time given by the timestamp is x, the delay is y,
	 * and the granularity is z, the first trigger will happen at x+y+z.
	 * 
	 * @param granularity
	 *            The granularity of the trigger. If this value is set to 2 the
	 *            policy will trigger at every second time point
	 * @param timestamp
	 *            The {@link TimeStamp} to measure the time with. This can be
	 *            either user defined of provided by the API.
	 * @param delay
	 *            A delay for the first trigger. If the start time given by the
	 *            timestamp is x, the delay is y, and the granularity is z, the
	 *            first trigger will happen at x+y+z.
	 */
	public TimeTriggerPolicy(long granularity, TimeStamp<DATA> timestamp, long delay) {
		this.startTime = timestamp.getStartTime() + delay;
		this.timestamp = timestamp;
		this.granularity = granularity;
	}

	@Override
	public boolean notifyTrigger(DATA datapoint) {
		return nextWindow(timestamp.getTimestamp(datapoint));
	}

	private boolean nextWindow(long recordTime) {
		// start time is included, but end time is excluded: >=
		if (recordTime >= startTime + granularity) {
			if (granularity != 0) {
				startTime = recordTime - ((recordTime - startTime) % granularity);
			}
			System.out.println("TRIGGERED on " + recordTime);
			return true;
		} else {
			return false;
		}
	}

}
