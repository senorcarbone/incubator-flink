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

import java.io.Serializable;

public interface NextGenEvictionPolicy<DATA> extends Serializable {

	/**
	 * Proves if and how many elements should be deleted from the element
	 * buffer. The eviction takes place after the trigger and after the call to
	 * the UDF but before the adding of the new datapoint.
	 *
	 * @param datapoint
	 *            datapoint the data point which arrived
	 * @param triggered
	 *            Information whether the UDF was triggered or not
	 * @param bufferSize
	 * @return The number of elements to be deleted from the buffer
	 */
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize);
}
