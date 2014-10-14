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

package org.apache.flink.streaming.util.nextGenExtractor;

import java.lang.reflect.Array;

import org.apache.flink.streaming.api.invokable.operator.NextGenExtractor;

public class FieldsFromArray<OUT> implements NextGenExtractor<Object, OUT[]> {

	/**
	 * Auto-generated version id
	 */
	private static final long serialVersionUID = 8075055384516397670L;
	private int[] order;
	private Class<OUT> clazz;
	
	public FieldsFromArray(Class<OUT> clazz,int... indexes) {
		this.order=indexes;
		this.clazz=clazz;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public OUT[] extract(Object in) {
		OUT[] output=(OUT[])Array.newInstance(clazz, order.length);
		for (int i=0;i<order.length;i++){
			output[i]=(OUT)Array.get(in, this.order[i]);
		}
		return output;
	}

}
