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

package org.apache.flink.streaming.examples.sampling.generators;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.examples.sampling.applications.Edge;
import org.apache.flink.streaming.examples.sampling.applications.Triangle;

import java.util.HashSet;

/**
 * Created by marthavk on 2015-03-11.
 */
public class Estimator<EV extends Edge, T extends Triangle> extends Tuple4<EV, EV, T, Integer> {

	public Estimator() {
		this.f3 = 0;
	}

	public Estimator(EV value0, EV value1, T value2, Integer value3) {
		super(value0, value1, value2, value3);
	}

	public EV getR1() {
		return this.f0;
	}

	public EV getR2() {
		return this.f1;
	}

	public T getTriangle() {
		return this.f2;
	}

	public Integer getC() {
		return this.f3;
	}

	public void setR1(EV r1) {
		this.f0 = r1;
	}

	public void setR2(EV r2) {
		this.f1 = r2;
	}

	public void setT(T t) {
		this.f2 = t;
	}

	public void incrementC() {
		this.f3++;
	}

	public boolean isTriangle(EV e) {
		HashSet h = new HashSet();
		h.addAll(this.getR1().getVertices());
		h.addAll(this.getR2().getVertices());
		//System.out.println("**" + h.toString());
		if (h.contains(e.getSrc()) && h.contains(e.getTrg())) {
			return true;
		}
		return false;
	}

	public void reset() {
		this.f0 = null;
		this.f1 = null;
		this.f2 = null;
		this.f3 = 0;
	}


}
