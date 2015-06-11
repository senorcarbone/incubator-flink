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
package org.apache.flink.streaming.sampling.samplers;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.sampling.helpers.Configuration;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by marthavk on 2015-04-07.
 */

public class ChainSampler<T> implements SampleFunction<T> {

	final double sampleRate;
	Chain<T, Long> chainSample;
	int windowSize;
	long counter;

	public ChainSampler(int lSize, int lWindowSize, double lSampleRate) {
		counter = 0;
		chainSample = new Chain<T, Long>(lSize);
		windowSize = lWindowSize;
		sampleRate = lSampleRate;
	}


	@Override
	public ArrayList<T> getElements() {
		return chainSample.extractSample();
	}

	@Override
	public void sample(T item) {
		counter++;
		Tuple2<T, Long> wrappedValue = new Tuple2<T, Long>(item, counter);
		storeChainedItems(wrappedValue);
		updateExpiredItems(wrappedValue);

		//sample item
		if (!chainSample.isFull()) {
			int pos = chainSample.getSize();
			chainSample.addSample(wrappedValue);

			long futureReplacement = selectReplacement(wrappedValue);
			Tuple2<T, Long> futureItem
					= new Tuple2<T, Long>(null, futureReplacement);
			chainSample.chainItem(futureItem, pos);

		} else {
			double prob = (double) chainSample.getMaxSize() / SamplingUtils.max(chainSample.getMaxSize(), wrappedValue.f1);
			if (SamplingUtils.flip(prob)) {

				int pos = SamplingUtils.randomBoundedInteger(0, chainSample.getSize() - 1);
				chainSample.replaceChain(pos, wrappedValue);

				long futureReplacement = selectReplacement(wrappedValue);
				Tuple2<T, Long> futureItem
						= new Tuple2<T, Long>(null, futureReplacement);

				chainSample.chainItem(futureItem, pos);
			}
		}
	}

	@Override
	public T getRandomEvent() {
		int randomIndex = SamplingUtils.nextRandInt(chainSample.getSize());
		return chainSample.get(randomIndex).getFirst().f0;
	}

	@Override
	public void reset() {
		this.chainSample.reset();
	}

	@Override
	public double getSampleRate() {
		return sampleRate;
	}

	@Override
	public String getFilename() {
		return Configuration.outputPath + "chain" + windowSize + "w" + chainSample.getMaxSize() + "bs";
	}


	/** CHAIN SAMPLING METHODS **/

	/**
	 * @return the index for replacement when current item expires
	 */
	public long selectReplacement(Tuple2<T, Long> item) {
		return SamplingUtils.randomBoundedLong(item.f1 + 1, item.f1 + windowSize);
	}

	/**
	 * Checks if the index of the current item has been selected in the past
	 * if so, it chains the item and updates all structures accordingly
	 *
	 * @param item
	 */
	void storeChainedItems(Tuple2<T, Long> item) {
		for (int i = 0; i < chainSample.getSize(); i++) {
			LinkedList<Tuple2<T, Long>> currentList = chainSample.get(i);
			if (currentList.getLast().f1.equals(item.f1)) {

				currentList.removeLast();
				chainSample.chainItem(item, i);

				long replacement = selectReplacement(item);
				Tuple2<T, Long> indicator = new Tuple2<T, Long>(null, replacement);
				chainSample.chainItem(indicator, i);
			}
		}
	}

	/**
	 * updates all expired Items (pops the heads of the chains so that
	 * the chained elements are now in the sample)
	 */
	void updateExpiredItems(Tuple2<T, Long> item) {

		int threshold = (int) (item.f1 - windowSize);
		for (int pos = 0; pos < chainSample.getSize(); pos++) {
			if (chainSample.get(pos).peek().f1 <= threshold) {
				chainSample.get(pos).pollFirst();
			}
		}
	}


	/**
	 * initialize priorityList and chainSample with null elements
	 */
	public void initializeList() {

		//initialize chainSample with null elements
		for (int i = 0; i < chainSample.getMaxSize(); i++) {
			chainSample.addSample(null);
		}

	}

	public String chainSampletoString(Chain<T, Long> chain) {
		String chainSampleStr;
		chainSampleStr = "[";
		Iterator<LinkedList> iter = chain.iterator();
		while (iter.hasNext()) {
			LinkedList<Tuple2<Object, Long>> list = iter.next();
			chainSampleStr += "(";
			if (!list.contains(null)) {
				for (int i = 0; i < list.size(); i++) {
					chainSampleStr += list.get(i).f1 + "->";
				}
			}
			chainSampleStr += ")";
		}
		chainSampleStr += "]";
		return chainSampleStr;
	}


}
