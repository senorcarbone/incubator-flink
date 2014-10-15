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

package org.apache.flink.streaming.util.deltafunction;

import org.apache.flink.streaming.api.invokable.operator.NextGenConversionAwareDeltaFunction;
import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class CosineDistance<DATA> extends NextGenConversionAwareDeltaFunction<DATA, double[]> {

	/**
	 * auto-generated id
	 */
	private static final long serialVersionUID = -1217813582965151599L;

	public CosineDistance() {
		super(null);
	}

	public CosineDistance(NextGenExtractor<DATA, double[]> converter) {
		super(converter);
	}

	@Override
	public double getNestedDelta(double[] oldDataPoint, double[] newDataPoint) {
		double sum1 = 0;
		double sum2 = 0;
		for (int i = 0; i < oldDataPoint.length; i++) {
			sum1 += oldDataPoint[i] * oldDataPoint[i];
			sum2 += newDataPoint[i] * newDataPoint[i];
		}
		sum1 = Math.sqrt(sum1);
		sum2 = Math.sqrt(sum2);

		return dotProduct(oldDataPoint, newDataPoint) / (sum1 * sum2);
	}

	private double dotProduct(double[] a, double[] b) {
		double result = 0;
		for (int i = 0; i < a.length; i++) {
			result += a[i] * b[i];
		}
		return result;
	}

}
