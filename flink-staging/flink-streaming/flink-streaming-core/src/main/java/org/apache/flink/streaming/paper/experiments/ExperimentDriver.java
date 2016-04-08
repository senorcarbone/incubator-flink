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
package org.apache.flink.streaming.paper.experiments;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingSensorPolicyGroup;
import org.apache.flink.streaming.api.windowing.windowbuffer.AggregationStats;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * This class reads an experiment setup generated by @link{DataGenerator} from a file and executes the experiments.
 * The results are written to a file.
 */
public abstract class ExperimentDriver {

	/**
	 * Specify below which cases you want to run
	 */
	protected static boolean RUN_PAIRS_LAZY = false;
	protected static boolean RUN_PAIRS_EAGER = false;
	protected static boolean RUN_B2B_LAZY = false;
	protected static boolean RUN_B2B_EAGER = false;
	protected static boolean RUN_GENERAL_LAZY = false;
	protected static boolean RUN_GENERAL_EAGER= false;
	protected static boolean RUN_PERIODIC_NO_PREAGG = false;
	protected static boolean RUN_PERIODIC_EAGER = false;
	protected static boolean RUN_B2B_NOT_PERIODIC_EAGER = false;
	protected static boolean RUN_GENERAL_NOT_PERIODIC_EAGER = false;
	protected static boolean RUN_PERIODIC_NO_PREAGG_EAGER = false;


	protected
	@SuppressWarnings("unchecked")
	LinkedList<Tuple3<String, Double, Double>>[] scenario;
	protected
	@SuppressWarnings("unchecked")
	LinkedList<Tuple3<String, Double, Double[]>>[] randomScenario;

	/**
	 * Specify the output file for the experiment results
	 */
	protected static String SETUP_PATH = "test-setup.txt";
	protected static String RESULT_PATH = "test-result.txt";


	public ExperimentDriver(String setupPath, String resultPath) {
		SETUP_PATH = setupPath;
		RESULT_PATH = resultPath;
	}

	public void execute() throws Exception {

		setupScenarios();

		AggregationStats stats = AggregationStats.getInstance();

		//Writer for the results
		PrintWriter resultWriter = new PrintWriter(RESULT_PATH, "UTF-8");
		resultWriter.println("SCEN\tCASE\tTIME\tAGG\tRED\tUPD\tMAXB\tAVGB\tUPD_AVG\tUPD_CNT\tMERGE_AVG\tMERGE_CNT");

		//run simple program to warm up (The first start up takes more time...)
		runWarmUpTask();

		//Variables needed in the loop
		runExperiments(stats, resultWriter);

		//close writer
		resultWriter.flush();
		resultWriter.close();
	}

	public abstract Extractor getCountExtractor();

	public abstract TimestampWrapper getTimeWrapper();

	public abstract Extractor getTimeExtractor();

	/**
	 * Runs a small warm up job. This is required because the first job needs longer to start up.
	 *
	 * @throws Exception Any exception which might happens during the execution
	 */
	public void runWarmUpTask() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		DataStream<Tuple3<Double, Double, Long>> source = env.addSource(new SimExperimentDriver.DataGenerator(10, 10));
		source.map((MapFunction<Tuple3<Double, Double, Long>, Long>) new MapFunction<Tuple3<Double, Double, Long>, Long>() {
			@Override
			public Long map(Tuple3<Double, Double, Long> value) throws Exception {
				return value.f2;
			}
		}).print();
		env.execute();
	}


	@SuppressWarnings("unchecked")
	List<DeterministicPolicyGroup> makeDeterministicPolicies(List<Tuple3<String, Double, Double>> settings) {

		List<DeterministicPolicyGroup> policyGroups = new ArrayList<>();
		for (Tuple3<String, Double, Double> setting : settings) {
			switch (setting.f0) {
				case "COUNT":
					policyGroups.add(new DeterministicPolicyGroup(
							new DeterministicCountTriggerPolicy<>(setting.f2.intValue(), getStartCount()),
							new DeterministicCountEvictionPolicy<>(setting.f1.intValue()),
							getCountExtractor()));
					break;
				case "TIME":
					policyGroups.add(new DeterministicPolicyGroup(
							new DeterministicTimeTriggerPolicy<>(setting.f2.intValue(), getTimeWrapper()),
							new DeterministicTimeEvictionPolicy<>(setting.f1.intValue(), getTimeWrapper()),
							getTimeExtractor()
					));
					break;
				case "PUNCT":
					policyGroups.add(new TumblingSensorPolicyGroup<>(new SensorPunctuationWindowDet(setting.f1.intValue())));
					break;
			}
		}

		return policyGroups;
	}

	@SuppressWarnings("unchecked")
	Tuple2<List<TriggerPolicy>, List<EvictionPolicy>> makeNonDeterministicPolicies(List<Tuple3<String, Double, Double>> settings) {

		List<TriggerPolicy> triggers = new ArrayList<>();
		List<EvictionPolicy> evictions = new ArrayList<>();
		
		for (Tuple3<String, Double, Double> setting : settings) {
			switch (setting.f0) {
				case "COUNT":
					triggers.add(new CountTriggerPolicy(setting.f2.intValue(), getStartCount()));
					evictions.add(new CountEvictionPolicy(setting.f1.intValue()));
					break;
				case "TIME":
					triggers.add(new TimeTriggerPolicy<>(setting.f2.intValue(), getTimeWrapper()));
					evictions.add(new TimeEvictionPolicy<>(setting.f1.intValue(), getTimeWrapper()));
					break;
				case "PUNCT":
					triggers.add(new SensorPunctuationWindowGen(setting.f1.intValue()));
					evictions.add(new SensorPunctuationWindowGen(setting.f1.intValue()));
			}
		}

		return new Tuple2<>(triggers,evictions);
	}


	protected abstract void runExperiments(AggregationStats stats, PrintWriter resultWriter) throws Exception;

	void setupExperiment(AggregationStats stats, PrintWriter resultWriter, JobExecutionResult result, int scenarioId, int caseId) {
		resultWriter.println(scenarioId + "\t" + caseId + "\t" + result.getNetRuntime() + "\t" + stats.getAggregateCount()
				+ "\t" + stats.getReduceCount() + "\t" + stats.getUpdateCount() + "\t" + stats.getMaxBufferSize() + "\t" + stats.getAverageBufferSize()
				+ "\t" + stats.getAverageUpdTime() + "\t" + stats.getTotalUpdateCount() + "\t" + stats.getAverageMergeTime() + "\t" + stats.getTotalMergeCount());
		stats.reset();
		resultWriter.flush();
	}


	private void setupScenarios() throws IOException {
		//Read the setup
		BufferedReader br = new BufferedReader(new FileReader(SETUP_PATH));
		String line;

		//Read the periodic setups
		scenario = new LinkedList[9];
		line = br.readLine();
		for (int i = 0; i < 9; i++) {
			scenario[i] = new LinkedList<>();
			//Skip separation lines between scenarios
			while (line != null && (!line.startsWith("SCENARIO " + (i + 1)))) {
				i++;
				scenario[i] = new LinkedList<>();
			}
			do {
				line = br.readLine();
			} while (!line.startsWith("\t\t"));
			//parse data rows
			do {
				String[] fields = line.split("\t");
				scenario[i].add(new Tuple3<>(fields[2], Double.parseDouble(fields[3]), Double.parseDouble(fields[4])));
				line = br.readLine();
			} while (line != null && line.startsWith("\t\t"));
		}

		//Read the random walk setup
		randomScenario = new LinkedList[9];
		if (line == null) {
			line = br.readLine();
		}
		for (int i = 0; i < 9; i++) {
			randomScenario[i] = new LinkedList<>();
			//Skip separation lines between scenarios
			while (line != null && (!line.startsWith("SCENARIO " + (i + 1)))) {
				i++;
				randomScenario[i] = new LinkedList<>();
			}

			//Stop loop if eof is reached (happens if there is no random walk setup present)
			if (line == null) {
				break;
			}

			do {
				line = br.readLine();
			} while (!line.startsWith("\t\t"));
			//parse data rows
			do {
				String[] fields = line.split("\t");
				String[] windowEndsString = fields[4].split(" ");
				Double[] windowEnds = new Double[windowEndsString.length];
				for (int j = 0; j < windowEndsString.length; j++) {
					windowEnds[j] = Double.parseDouble(windowEndsString[j]);
				}
				randomScenario[i].add(new Tuple3<>(fields[2], Double.parseDouble(fields[3]), windowEnds));
				line = br.readLine();
			} while (line != null && line.startsWith("\t\t"));
		}
	}

	public abstract int getStartCount();
}