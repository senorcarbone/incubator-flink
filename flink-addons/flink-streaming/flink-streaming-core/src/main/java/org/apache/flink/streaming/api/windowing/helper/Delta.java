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

import org.apache.flink.streaming.api.windowing.deltafunction.CosineDistance;
import org.apache.flink.streaming.api.windowing.deltafunction.EuclideanDistance;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.apache.flink.streaming.api.windowing.extractor.ArrayFromTuple;
import org.apache.flink.streaming.api.windowing.extractor.ConcatinatedExtract;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.extractor.FieldsFromArray;
import org.apache.flink.streaming.api.windowing.policy.DeltaPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * This helper represents a trigger or eviction policy based on a
 * {@link DeltaFunction}.
 * 
 * @param <DATA>
 *            the data type handled by the delta function represented by this
 *            helper.
 */
public class Delta<DATA> implements WindowingHelper<DATA> {

	private DeltaFunction<DATA> deltaFunction;
	private DATA initVal;
	private double threshold;

	/**
	 * Creates a delta helper representing a delta count or eviction policy
	 * 
	 * @param deltaFunction
	 *            The delta function which should be used to calculate the delta
	 *            between points.
	 * @param initVal
	 *            The initial value which will be used to calculate the first
	 *            delta.
	 * @param threshold
	 *            The threshold used by the delta function.
	 */
	public Delta(DeltaFunction<DATA> deltaFunction, DATA initVal, double threshold) {
		this.deltaFunction = deltaFunction;
		this.initVal = initVal;
		this.threshold = threshold;
	}

	@Override
	public EvictionPolicy<DATA> toEvict() {
		return new DeltaPolicy<DATA>(deltaFunction, initVal, threshold);
	}

	@Override
	public TriggerPolicy<DATA> toTrigger() {
		return new DeltaPolicy<DATA>(deltaFunction, initVal, threshold);
	}

	/**
	 * Creates a delta helper representing a delta count or eviction policy
	 * 
	 * @param deltaFunction
	 *            The delta function which should be used to calculate the delta
	 *            between points.
	 * @param initVal
	 *            The initial value which will be used to calculate the first
	 *            delta.
	 * @param threshold
	 *            The threshold used by the delta function.
	 * @return a delta helper representing a delta count or eviction policy
	 */
	public static <DATA> Delta<DATA> of(DeltaFunction<DATA> deltaFunction, DATA initVal,
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

		/**
		 * Use this to set extraction: The given field will be extracted from
		 * the data. The delta is then calculated on the extracted field.
		 * 
		 * @param id
		 *            the id of the field to be extracted.
		 * @return Another helper with methods to do further settings.
		 */
		public DeltaHelper2 onField(int id) {
			return new DeltaHelper2(threshold, id);
		}

		/**
		 * Use this to set extraction: The given fields will be extracted from
		 * the data. The delta is then calculated on the extracted fields.
		 * 
		 * @param id
		 *            the ids of the fields to be extracted. Possibly in any
		 *            user defined order.
		 * @return Another helper with methods to do further settings.
		 */
		public DeltaHelper2 onFields(int... id) {
			return new DeltaHelper2(threshold, id);
		}

		/**
		 * Use this to specify that the distance should be measured with
		 * euclidean distance without further extraction before.
		 * 
		 * @return Another helper with methods to do further settings.
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper4 measuredWithEucledianDistance() {
			return new DeltaHelper4(threshold, new EuclideanDistance(null));
		}

		/**
		 * Use this to specify that the distance should be measured with cosine
		 * distance without further extraction before.
		 * 
		 * @return Another helper with methods to do further settings.
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper4 measuredWithCosineDistance() {
			return new DeltaHelper4(threshold, new CosineDistance(null));
		}

		/**
		 * Use this to specify that the distance should be measured with a delta
		 * function provided as parameter without further extraction before.
		 * 
		 * @param delta the delta function to be used.
		 * @return Another helper with methods to do further settings.
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper4 measuredWithCustomFunction(DeltaFunction delta) {
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

		/**
		 * Define that the input of the extraction is a tuple type
		 * @return Another helper with methods to do further settings.
		 */
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

		/**
		 * Define that the input of the extraction is an array
		 * @return Another helper with methods to do further settings.
		 */
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

		/**
		 * Use this to set extraction: The given field will be extracted from
		 * the data. The delta is then calculated on the extracted field.
		 * 
		 * @param id
		 *            the id of the field to be extracted.
		 * @return Another helper with methods to do further settings.
		 */
		public DeltaHelper2 fromField(int field) {
			return new DeltaHelper2(extractor, threshold, field);
		}

		/**
		 * Use this to set extraction: The given fields will be extracted from
		 * the data. The delta is then calculated on the extracted fields.
		 * 
		 * @param id
		 *            the ids of the fields to be extracted. Possibly in any
		 *            user defined order.
		 * @return Another helper with methods to do further settings.
		 */
		public DeltaHelper2 fromFields(int... fields) {
			return new DeltaHelper2(extractor, threshold, fields);
		}

		/**
		 * Use this to specify that the distance should be measured with
		 * euclidean distance without further extraction before.
		 * 
		 * @return Another helper with methods to do further settings.
		 */
		@SuppressWarnings("unchecked")
		public DeltaHelper4<FROM> measuredWithEucledianDistance() {
			return new DeltaHelper4<FROM>(threshold, new EuclideanDistance<FROM>(extractor));
		}

		/**
		 * Use this to specify that the distance should be measured with cosine
		 * distance without further extraction before.
		 * 
		 * @return Another helper with methods to do further settings.
		 */
		@SuppressWarnings("unchecked")
		public DeltaHelper4<FROM> measuredWithCosineDistance() {
			return new DeltaHelper4<FROM>(threshold, new CosineDistance<FROM>(extractor));
		}
		
		/**
		 * Use this to specify that the distance should be measured with a delta
		 * function provided as parameter without further extraction before.
		 * 
		 * @param delta the delta function to be used.
		 * @return Another helper with methods to do further settings.
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DeltaHelper4 measuredWithCustomFunction(DeltaFunction delta) {
			return new DeltaHelper4(threshold, delta);
		}
	}

	public static class DeltaHelper4<DATA> {
		private double threshold;
		private DeltaFunction<DATA> deltaFunction;

		private DeltaHelper4(double threshold, DeltaFunction<DATA> deltaFunction) {
			this.threshold = threshold;
			this.deltaFunction = deltaFunction;
		}

		/**
		 * Set the initial value used to calculate the first delta.
		 * @param initialValue the initial value used to calculate the first delta.
		 * @return the delta helper representing the delta trigger or eviction policy.
		 */
		public Delta<DATA> initializedWith(DATA initialValue) {
			return new Delta<DATA>(deltaFunction, initialValue, threshold);
		}
	}
}
