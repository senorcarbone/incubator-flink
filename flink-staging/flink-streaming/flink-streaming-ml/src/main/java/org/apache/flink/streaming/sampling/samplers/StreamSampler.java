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

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * Created by marthavk on 2015-06-01.
 */
public class StreamSampler<IN> extends AbstractUdfStreamOperator<IN, SampleFunction<IN>>
		implements OneInputStreamOperator<IN, IN> {

	final SampleFunction<IN> sampler;
	final double sampleRate;
	boolean running;
	long millis;
	int nanos;
	long counter = 0;
	final boolean printToFile = false;

	/**
	 * write to file *
	 */
	File file;
	String filename;
	//BufferedWriter bw;
	FileWriter fw;
	PrintWriter pw;

	public StreamSampler(SampleFunction<IN> userFunction) {
		super(userFunction);
		this.sampler = userFunction;
		sampleRate = sampler.getSampleRate();
		if (printToFile) {
			filename = sampler.getFilename();
		}
		setTimeIntervals();
	}

	@Override
	public void open(Configuration parameters) throws Exception {

		/** write to file **/
		super.open(parameters);
		if (printToFile) {
			file = new File(filename);
			if (!file.exists()) {
				file.createNewFile();
			}
			fw = new FileWriter(file.getAbsolutePath(), true);
			pw = new PrintWriter(fw);
		}

		running = true;
		FunctionUtils.setFunctionRuntimeContext(sampler, runtimeContext);
		FunctionUtils.openFunction(sampler, parameters);

		//logic for the thread
		final Thread thread = new Thread(new Runnable() {

			@Override
			public void run() {
				while (running) {
					try {
						Thread.sleep(millis, nanos);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						if (running) {
							//TODO: job finishes before thread -> NullPointerException
							output.collect(sampler.getRandomEvent());
						}
					} catch (IndexOutOfBoundsException ignored) {
					} catch (IllegalArgumentException ignored) {
					} catch (NullPointerException ignored) {
					}
				}
			}
		});

		thread.start();
	}

	@Override
	public void close() throws Exception {
		super.close();
		//running = false;
		if (printToFile) {
			pw.close();
		}

	}

	@Override
	public void processElement(IN element) throws Exception {
		counter++;
		if (counter % 25000 == 0) {
			System.out.println(userFunction.getClass() + " " + counter);
		}
		sampler.sample(element);
		if (printToFile) {
			pw.println(getDistributionOfBuffer());
		}
	}

	private void setTimeIntervals() {
		double number = 1000.0 / sampleRate;
		millis = (long) number;
		long period = Math.round(1000000000.0 / sampleRate);
		nanos =(int) (period - millis*1000000);
		System.out.println("sample rate " + sampleRate);
		System.out.println("period " + period);
		System.out.println("millis " + millis);
		System.out.println("nanos " + nanos);
	}

	private	 synchronized String getDistributionOfBuffer() {
		SummaryStatistics stats = SamplingUtils.getStats((ArrayList<Double>) sampler.getElements());
		Tuple2<Double, Double> outTuple = new Tuple2<Double, Double>(stats.getMean(), stats.getStandardDeviation());
		return outTuple.toString();
	}

}

