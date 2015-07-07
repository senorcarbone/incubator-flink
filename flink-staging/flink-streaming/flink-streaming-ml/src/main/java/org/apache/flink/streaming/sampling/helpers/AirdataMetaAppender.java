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
package org.apache.flink.streaming.sampling.helpers;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.sampling.helpers.StreamTimestamp;

/**
 * Created by marthavk on 2015-05-27.
 */
public class AirdataMetaAppender<T extends Tuple8<Integer, Integer, Integer, String, String, String, Integer, Integer>> extends RichMapFunction<T, Tuple3<T, StreamTimestamp, Long>> {

	long index = 0;

	@Override
	public Tuple3<T, StreamTimestamp, Long> map(T value) throws Exception {

		/*//value
		Double rand = value.generate();*/

		//timestamp
		final StreamTimestamp t = new StreamTimestamp(value.f0);

		//order
		index++;

		return new Tuple3<T, StreamTimestamp, Long>(value, t, index);
	}


}