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

import org.apache.flink.streaming.sampling.helpers.Configuration;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.util.ArrayList;

/**
 * Created by marthavk on 2015-06-02.
 */
public class UniformSampler<IN> implements SampleFunction<IN> {

	final int sampleRate;
	Reservoir<IN> reservoir;
	long counter = 0;

	public UniformSampler(int lMaxSize, int lRate) {
		reservoir = new Reservoir<IN>(lMaxSize);
		sampleRate = lRate;
	}

	@Override
	public synchronized ArrayList<IN> getElements() {
		return reservoir.getSample();
	}

	@Override
	public synchronized void sample(IN element) {
		counter++;
		if (SamplingUtils.flip((double) reservoir.getMaxSize() / counter)) {
			if (!reservoir.isFull()) {
				reservoir.addSample(element);
			} else {
				replace(element);
			}
		}
	}

	@Override
	public synchronized IN getRandomEvent() throws IndexOutOfBoundsException {
		if (reservoir.getSize() > 0) {
			IN randomRecord = reservoir.getSample().get(SamplingUtils.nextRandInt(reservoir.getSize()));
			return randomRecord;
		} else {
			throw new IndexOutOfBoundsException();
		}

	}

	@Override
	public synchronized void reset() {
		reservoir.getSample().clear();
		counter = 0;

	}

	@Override
	public double getSampleRate() {
		return sampleRate;
	}

	@Override
	public String getFilename() {
		return Configuration.outputPath + "reservoir" + reservoir.getMaxSize();
	}

	public synchronized void replace(IN item) {
		// choose position in sample uniformly at random
		int pos = SamplingUtils.nextRandInt(reservoir.getSize());
		// replace element at pos with item
		reservoir.replaceSample(pos, item);
	}


}
