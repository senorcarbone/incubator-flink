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

package org.apache.flink.streaming.examples.sampling.samplers;


import org.apache.flink.streaming.examples.sampling.SamplingUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by marthavk on 2015-03-05.
 */
public class Reservoir<T> implements Serializable, Iterable, Sampler<T> {

	private ArrayList<T> reservoir;
	private final int maxSize;
	private int samplingFactor;


	public Reservoir(int size) {
		maxSize = size;
		reservoir = new ArrayList<T>(size);
	}

	/**
	 * GETTERS AND SETTERS*
	 */

	public int getMaxSize() {
		return maxSize;
	}

	@Override
	public int size() {
		return reservoir.size();
	}

	public void setReservoir(ArrayList<T> reservoir) {
		this.reservoir = reservoir;
	}


	@Override
	public T[] getElements() {
		return (T[]) reservoir.toArray();
	}

	/**
	 * INSERT & REPLACE METHODS *
	 */

	@Override
	public void sample(T element) {
		samplingFactor++;
		if (SamplingUtils.flip(samplingFactor / maxSize)) {
			if (!isFull()) reservoir.add(element);
			else replaceElement(element);
		}
	}

	@Override
	public Iterator iterator() {
		return reservoir.iterator();
	}

	/**
	 * Chooses an existing element uniformly at random and replaces it with e
	 *
	 * @param e
	 */
	private void replaceElement(T e) {
		if (isFull()) {
			// choose position uniformly at random
			int pos = new Random().nextInt(maxSize);
			// replace element at pos
			reservoir.set(pos, e);
		} else throw new ArrayIndexOutOfBoundsException();
	}

	/**
	 * AUXILIARY METHODS *
	 */
	private boolean isFull() {
		return reservoir.size() == maxSize;
	}


	@Override
	public String toString() {
		return reservoir.toString();
	}


	private void print() {
		System.out.println(reservoir.toString());
	}

	/**
	 * MERGE METHODS *
	 */
	public static Reservoir merge(Reservoir r1, Reservoir r2) {
		Reservoir rout = new Reservoir(r1.maxSize + r2.maxSize);
		rout.reservoir.addAll(r1.reservoir);
		rout.reservoir.addAll(r2.reservoir);
		return rout;
	}

	//It's better to keep operations immutable
//	public void mergeWith(Reservoir<T> r1) {
//		this.setMaxSize(r1.getMaxSize() + this.getMaxSize());
//		ArrayList<T> newReservoir = new ArrayList<T>();
//		newReservoir.addAll(this.getReservoir());
//		newReservoir.addAll(r1.getReservoir());
//	}

}
