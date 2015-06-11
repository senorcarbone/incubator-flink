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
 * Biased Reservoir Sampler (uses a Reservoir but is biased towards new events).
 * Each new element will deterministically enter the reservoir. The Reservoir is either extended
 * (until it reaches max size) or the incoming element is replacing an older element in the
 * reservoir uniformly at random.
 *
 * @param <IN> the type of incoming elements
 */
public class BiasedReservoirSampler<IN> implements SampleFunction<IN> {

	final double sampleRate;
	Reservoir<IN> reservoir;
	int counter = 0;

	public BiasedReservoirSampler(int maxsize, double lSampleRate) {
		reservoir = new Reservoir<IN>(maxsize);
		sampleRate = lSampleRate;
	}

	@Override
	public ArrayList<IN> getElements() {
		return reservoir.getSample();
	}

	@Override
	public void sample(IN element) {
		counter++;
		double proportion = reservoir.getSize() / reservoir.getMaxSize();
		if (SamplingUtils.flip(proportion)) {
			reservoir.replaceSample(element);
		} else {
			reservoir.addSample(element);
		}
	}

	@Override
	public IN getRandomEvent() {
		int randomIndex = SamplingUtils.nextRandInt(reservoir.getSize());
		return reservoir.get(randomIndex);
	}

	@Override
	public void reset() {
		reservoir.reset();
	}

	@Override
	public double getSampleRate() {
		return sampleRate;
	}

	@Override
	public String getFilename() {
		return Configuration.outputPath + "biased" + reservoir.getMaxSize();
	}
}
