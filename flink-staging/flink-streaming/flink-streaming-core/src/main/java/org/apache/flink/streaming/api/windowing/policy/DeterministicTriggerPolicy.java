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

/**
 * A deterministic trigger policy allows to apply border to border
 * pre-aggregation means. Trigger policies are deterministic if they can
 * calculate the end (upper border) of the next window, given the end position
 * of the previous window.
 */
public interface DeterministicTriggerPolicy<DATA> extends TriggerPolicy<DATA> {

	/**
	 * This methods calculated the next upper border, thus, it calculates the
	 * position where the next window will end beyond the given position.
	 * 
	 * @param previouseTriggerPosition
	 *            the position of the previous window end
	 * @return the position of the next window end after the position given as
	 *         parameter
	 */
	public double getNextTriggerPosition(double previouseTriggerPosition);

}
