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

package org.apache.flink.streaming.util.nextGenDeltaFunction;

import org.apache.flink.streaming.api.invokable.operator.NextGenConversionAwareDeltaFunction;
import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class EuclideanDistance<DATA> extends NextGenConversionAwareDeltaFunction<DATA, double[]>{

	public EuclideanDistance(){
		super(null);
	}
	
	public EuclideanDistance(NextGenExtractor<DATA, double[]> converter) {
		super(converter);
	}

	/**
	 * auto-generated version id
	 */
	private static final long serialVersionUID = 3119432599634512359L;

	@Override
	public double getNestedDelta(double[] oldDataPoint, double[] newDataPoint) {
		double result=0;
		for (int i=0;i<oldDataPoint.length;i++){
			result+=(oldDataPoint[i]-newDataPoint[i])*(oldDataPoint[i]-newDataPoint[i]);
		}
		return Math.sqrt(result);
	}

}
