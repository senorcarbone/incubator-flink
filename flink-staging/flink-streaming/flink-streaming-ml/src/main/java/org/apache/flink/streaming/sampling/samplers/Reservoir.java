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

package org.apache.flink.streaming.sampling.samplers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by marthavk on 2015-03-05.
 */
public class Reservoir<T> extends Buffer<T> implements Serializable, Iterable {

	public Reservoir(int size) {
		super(size);
	}

	/**
	 * MERGE METHODS
	 */
	public static Reservoir merge(Reservoir r1, Reservoir r2) {
		Reservoir rout = new Reservoir(r1.maxSize + r2.maxSize);
		rout.sample.addAll(r1.sample);
		rout.sample.addAll(r2.sample);
		return rout;
	}

	public void addSample(T item) {
		if (!isFull()) {
			this.sample.add(item);
		} else {
			replaceSample(item);
		}
	}

	/**
	 * Chooses an existing element uniformly at random and replaces it with e
	 *
	 * @param item
	 */
	public void replaceSample(T item) {

		// choose position uniformly at random
		int pos = new Random().nextInt(sample.size());
		// replace element at pos
		this.replaceSample(pos, item);

	}

	public void reset() {
		this.sample.clear();
	}

	/**
	 * AUXILIARY METHODS
	 */

	@Override
	public String toString() {
		return sample.toString();
	}

	public void mergeWith(Reservoir<T> r1) {
		this.setMaxSize(r1.getMaxSize() + this.getMaxSize());
		ArrayList<T> newReservoir = new ArrayList<T>();
		newReservoir.addAll(this.getSample());
		newReservoir.addAll(r1.getSample());
	}

	@Override
	public Iterator iterator() {
		return sample.iterator();
	}


}
