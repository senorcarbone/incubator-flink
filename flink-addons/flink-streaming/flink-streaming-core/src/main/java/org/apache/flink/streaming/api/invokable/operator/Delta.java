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

import org.apache.flink.streaming.api.windowing.deltafunction.CosineDistance;
import org.apache.flink.streaming.api.windowing.deltafunction.EuclideanDistance;
import org.apache.flink.streaming.api.windowing.extractor.ArrayFromTuple;
import org.apache.flink.streaming.api.windowing.extractor.ConcatinatedExtract;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.extractor.FieldsFromArray;

public class Delta<DATA> implements NextGenWindowHelper<DATA> {

	private NextGenDeltaFunction<DATA> deltaFunction;
	private DATA initVal;
	private double threshold;

	public Delta(NextGenDeltaFunction<DATA> deltaFunction, DATA initVal, double threshold) {
		this.deltaFunction = deltaFunction;
		this.initVal = initVal;
		this.threshold = threshold;
	}

	@Override
	public NextGenEvictionPolicy<DATA> toEvict() {
		return new NextGenDeltaPolicy<DATA>(deltaFunction, initVal, threshold);
	}

	@Override
	public NextGenTriggerPolicy<DATA> toTrigger() {
		return new NextGenDeltaPolicy<DATA>(deltaFunction, initVal, threshold);
	}

	public static <DATA> Delta<DATA> of(NextGenDeltaFunction<DATA> deltaFunction, DATA initVal,
			double threshold) {
		return new Delta<DATA>(deltaFunction, initVal, threshold);
	}

	/**
	 * This method allows to set up the delta function in a readable way. The
	 * syntax is the following:<br>
	 * <code>
	 * Delta.of([threshold])
	 * |-> .onField([id])/onFields([id...])
	 * |    |->.fromArray()  <-------------------------,
	 * |    '->.tromTuple()  <-------------------------|
	 * |        |->fromField([id])/fromFields([id...])-'
	 * |        v
	 * '-> .measuredWith...().initializedWith([DATA])
	 * </code>
	 * 
	 * @param threshold
	 *            the threshold for the delta function
	 * @return instance of a helper class which provides further methods to make
	 *         all settings in a readable way.
	 */
	public static DeltaHelper1 of(double threshold) {
		return new DeltaHelper1(threshold);
	}

	public static class DeltaHelper1 {
		private double threshold;

		private DeltaHelper1(double threshold) {
			this.threshold = threshold;
		}

		public DeltaHelper2 onField(int id) {
			return new DeltaHelper2(threshold, id);
		}

		public DeltaHelper2 onFields(int... id) {
			return new DeltaHelper2(threshold, id);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper4 measuredWithEucledianDistance() {
			return new DeltaHelper4(threshold, new EuclideanDistance(null));
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper4 measuredWithCosineDistance() {
			return new DeltaHelper4(threshold, new CosineDistance(null));
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper4 measuredWithCustomFunction(NextGenDeltaFunction delta) {
			return new DeltaHelper4(threshold, delta);
		}
	}

	public static class DeltaHelper2 {
		private double threshold;
		private int[] fields;
		@SuppressWarnings("rawtypes")
		private Extractor laterExtractor;

		private DeltaHelper2(double threshold, int... fields) {
			this(null, threshold, fields);
		}

		@SuppressWarnings("rawtypes")
		private DeltaHelper2(Extractor laterExtractor, double threshold, int... fields) {
			this.laterExtractor = laterExtractor;
			this.threshold = threshold;
			this.fields = fields;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper3 fromTuple() {
			if (laterExtractor == null) {
				// return new DeltaHelper3(threshold, new
				// FieldsFromTuple(fields));
				return new DeltaHelper3(threshold, new ArrayFromTuple(fields));
			} else {
				// return new DeltaHelper3(threshold, new
				// ConcatinatedExtract(new FieldsFromTuple(fields),
				// laterExtractor));
				return new DeltaHelper3(threshold, new ConcatinatedExtract(new ArrayFromTuple(
						fields), laterExtractor));
			}

		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper3 fromArray(Class clazz) {
			if (laterExtractor == null) {
				return new DeltaHelper3(threshold, new FieldsFromArray(clazz, fields));
			} else {
				return new DeltaHelper3(threshold, new ConcatinatedExtract(new FieldsFromArray(
						clazz, fields), laterExtractor));
			}
		}
	}

	public static class DeltaHelper3<FROM> {

		@SuppressWarnings("rawtypes")
		private Extractor extractor;
		private double threshold;

		private <TO> DeltaHelper3(double threshold, Extractor<FROM, TO> extractor) {
			this.threshold = threshold;
			this.extractor = extractor;
		}

		public DeltaHelper2 fromField(int field) {
			return new DeltaHelper2(extractor, threshold, field);
		}

		public DeltaHelper2 fromFields(int... fields) {
			return new DeltaHelper2(extractor, threshold, fields);
		}

		@SuppressWarnings("unchecked")
		public DeltaHelper4<FROM> measuredWithEucledianDistance() {
			return new DeltaHelper4<FROM>(threshold, new EuclideanDistance<FROM>(extractor));
		}

		@SuppressWarnings("unchecked")
		public DeltaHelper4<FROM> measuredWithCosineDistance() {
			return new DeltaHelper4<FROM>(threshold, new CosineDistance<FROM>(extractor));
		}

	}

	public static class DeltaHelper4<DATA> {
		private double threshold;
		private NextGenDeltaFunction<DATA> deltaFunction;

		private DeltaHelper4(double threshold, NextGenDeltaFunction<DATA> deltaFunction) {
			this.threshold = threshold;
			this.deltaFunction = deltaFunction;
		}

		public Delta<DATA> initializedWith(DATA initialValue) {
			return new Delta<DATA>(deltaFunction, initialValue, threshold);
		}
	}
}
