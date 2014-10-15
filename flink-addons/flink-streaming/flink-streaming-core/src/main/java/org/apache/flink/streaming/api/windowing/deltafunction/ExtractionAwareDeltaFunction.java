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

package org.apache.flink.streaming.api.windowing.deltafunction;

import org.apache.flink.streaming.api.windowing.extractor.Extractor;

public abstract class ExtractionAwareDeltaFunction<DATA, TO> implements
		DeltaFunction<DATA> {

	/**
	 * Generated Version ID
	 */
	private static final long serialVersionUID = 6927486219702689554L;
	private Extractor<DATA, TO> converter;

	public ExtractionAwareDeltaFunction(Extractor<DATA, TO> converter) {
		this.converter = converter;
	}

	@SuppressWarnings("unchecked")
	// see comment below
	@Override
	public double getDelta(DATA oldDataPoint, DATA newDataPoint) {
		if (converter == null) {
			// In case no conversion/extraction is required, we can cast DATA to
			// TO
			// => Therefore, "unchecked" warning is suppressed for this method.
			return getNestedDelta((TO) oldDataPoint, (TO) newDataPoint);
		} else {
			return getNestedDelta(converter.extract(oldDataPoint), converter.extract(newDataPoint));
		}

	}

	public abstract double getNestedDelta(TO oldDataPoint, TO newDataPoint);

}
