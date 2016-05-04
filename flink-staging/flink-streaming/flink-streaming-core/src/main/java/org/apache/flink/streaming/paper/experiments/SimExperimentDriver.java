package org.apache.flink.streaming.paper.experiments;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.AggregationStats;
import org.apache.flink.streaming.paper.AggregationFramework;
import org.apache.flink.streaming.paper.PaperExperiment;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class SimExperimentDriver extends ExperimentDriver {

	{
		RUN_B2B_LAZY = true;
		RUN_B2B_EAGER = true;
		RUN_GENERAL_LAZY = true;
		RUN_PERIODIC_NO_PREAGG = true;
		RUN_PERIODIC_EAGER = true;
		RUN_B2B_NOT_PERIODIC_EAGER = true;
		RUN_GENERAL_NOT_PERIODIC_EAGER = true;
		RUN_PERIODIC_NO_PREAGG_EAGER = true;
	}

	/**
	 * Specify the number of tuples you want to process
	 * -1 for a full execution (e.g. streaming a whole file)
	 */
	private int NUM_TUPLES = 1000000;

	/**
	 * Set a sleep period in ms.
	 * The data source will pause between input emissions for the given time.
	 */
	private final int SOURCE_SLEEP = 0;

	public SimExperimentDriver(String setupPath, String resultPath) {
		super(setupPath, resultPath);
	}

	private static class CountExtractor implements Extractor<Tuple3<Double, Double, Long>, Double> {
		@Override
		public Double extract(Tuple3<Double, Double, Long> in) {
			return in.f1;
		}
	}

	private static class TimeExtractor implements Extractor<Tuple3<Double, Double, Long>, Double> {
		@Override
		public Double extract(Tuple3<Double, Double, Long> in) {
			return in.f2.doubleValue();
		}
	}
	
	private static class SimTimestamp implements Timestamp<Tuple3<Double, Double, Long>> {
		@Override
		public long getTimestamp(Tuple3<Double, Double, Long> value) {
			return value.f2;
		}
	}

	@Override
	public Extractor getCountExtractor() {
		return new CountExtractor();
	}

	@Override
	public TimestampWrapper getTimeWrapper() {
		return new TimestampWrapper(new SimTimestamp(), 0l);
	}

	@Override
	public Extractor getTimeExtractor() {
		return new TimeExtractor();
	}


	@Override
	public void runExperiments(AggregationStats stats, PrintWriter resultWriter) throws Exception {


		JobExecutionResult result;//Run experiments for different scenarios
		// Scenario 1 (i=0): regular range, regular maxSlide
		// Scenario 2 (i=1): low range, regular maxSlide
		// Scenario 3 (i=2): high range, regular maxSlide
		// Scenario 4 (i=3): regular range, low maxSlide
		// Scenario 5 (i=4): regular range, high maxSlide
		// Scenario 6 (i=5): high range, high maxSlide
		// Scenario 7 (i=6): low range, high maxSlide
		// Scenario 8 (i=7): low range, low maxSlide
		// Scenario 9 (i=8): high range, low maxSlide
		//
		//The differen Algorithms are the following
		// CASE 0: Pairs LAZY - original
		// CASE 1: deterministic policy groups; periodic; LAZY
		// CASE 2: deterministic policy groups; not periodic; LAZY
		// CASE 3: not deterministic; not periodic; LAZY
		// CASE 4: not deterministic; periodic; LAZY (theoretically periodic & deterministic, but periodicity is not utilized)
		// CASE 5: Same as case 0 but EAGER
		// CASE 6: Same as case 1 but EAGER
		// CASE 7: Same as case 2 but EAGER
		// CASE 8: Same as case 3 but EAGER
		// CASE 9: Same as case 4 but EAGER
		for (int i = 0; i < 9; i++) {
			//check if the scenarios is present in the setup file, otherwise skip this iterations
			if (scenarios.get(i) == null || scenarios.get(i).size() == 0) {
				continue;
			}

			int testCase = 0;

			if (RUN_PAIRS_LAZY) {

                /*
				 * Case 0
                 */

				StreamExecutionEnvironment env0 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source2 = env0.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> periodicPolicyGroups = makePeriodicPolicyGroups(scenarios.get(i));
				SumAggregation.applyOn(source2, new Tuple3<>(periodicPolicyGroups, new LinkedList<>(),
								new LinkedList<>()), AggregationFramework.AGGREGATION_STRATEGY.LAZY,
						AggregationFramework.DISCRETIZATION_TYPE.PAIRS)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env0.execute("Scanario " + i + " Case " + testCase);

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;

			if (RUN_B2B_LAZY) {

                /*
				 * Case 1
                 */

				StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source2 = env2.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> periodicPolicyGroups = makePeriodicPolicyGroups(scenarios.get(i));
				SumAggregation.applyOn(source2, new Tuple3<>(periodicPolicyGroups, new LinkedList<>(),
								new LinkedList<>()), AggregationFramework.AGGREGATION_STRATEGY.LAZY,
						AggregationFramework.DISCRETIZATION_TYPE.B2B)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env2.execute("Scanario " + i + " Case " + testCase);

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;

			if (RUN_B2B_EAGER && randomScenarios.get(i) != null && randomScenarios.get(i).size() > 0) {
				/*
				 * Evaluate with deterministic policy groups (not periodic)  (case 2)
                 */

				StreamExecutionEnvironment env3 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source3 = env3.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> randomWalkPolicyGroups = makeRandomWalkPolicyGroups(randomScenarios.get(i));
				SumAggregation.applyOn(source3, new Tuple3<>(randomWalkPolicyGroups, new LinkedList<>(),
								new LinkedList<>()), AggregationFramework.AGGREGATION_STRATEGY.LAZY,
						AggregationFramework.DISCRETIZATION_TYPE.B2B)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env3.execute();

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;


			if (RUN_GENERAL_LAZY && randomScenarios.get(i) != null && randomScenarios.get(i).size() > 0) {
                /*
                 *Evaluate not deterministic version  (case 3)
                 */

				StreamExecutionEnvironment env4 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source4 = env4.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				SumAggregation.applyOn(source4, new Tuple3<>(new LinkedList<>(), makeNDRandomWalkTrigger(randomScenarios.get(i)),
								makeNDRandomWalkEviction(randomScenarios.get(i))), AggregationFramework.AGGREGATION_STRATEGY.LAZY,
						AggregationFramework.DISCRETIZATION_TYPE.B2B)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env4.execute();

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;

			if (RUN_PERIODIC_NO_PREAGG) {
                /*
                 *Evaluate periodic setup, but without any pre-aggregation (case 4)
                 */

				StreamExecutionEnvironment env5 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source4 = env5.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				SumAggregation.applyOn(source4, new Tuple3<>(new LinkedList<>(), makeNDPeriodicTrigger(scenarios.get(i)),
						makeNDPeriodicEviction(scenarios.get(i))), AggregationFramework.AGGREGATION_STRATEGY.LAZY, AggregationFramework.DISCRETIZATION_TYPE.B2B)
							.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env5.execute();

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;

			if (RUN_PAIRS_EAGER) {
                /*
                 * Evaluate with deterministic policy groups (periodic) EAGER version  (case 5)
                 */

				StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source2 = env1.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> periodicPolicyGroups = makePeriodicPolicyGroups(scenarios.get(i));
				SumAggregation.applyOn(source2, new Tuple3<>(periodicPolicyGroups, new LinkedList<>(),
								new LinkedList<>()), AggregationFramework.AGGREGATION_STRATEGY.EAGER,
						AggregationFramework.DISCRETIZATION_TYPE.PAIRS)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env1.execute("Scanario " + i + " Case " + testCase);

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;

			if (RUN_PERIODIC_EAGER) {
                /*
                 * Evaluate with deterministic policy groups (periodic) EAGER version  (case 6)
                 */

				StreamExecutionEnvironment env6 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source2 = env6.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> periodicPolicyGroups = makePeriodicPolicyGroups(scenarios.get(i));
				SumAggregation.applyOn(source2, new Tuple3<>(periodicPolicyGroups, new LinkedList<>(),
								new LinkedList<>()), AggregationFramework.AGGREGATION_STRATEGY.EAGER,
						AggregationFramework.DISCRETIZATION_TYPE.B2B)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env6.execute("Scanario " + i + " Case " + testCase);

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;

			if (RUN_B2B_NOT_PERIODIC_EAGER && randomScenarios.get(i) != null && randomScenarios.get(i).size() > 0) {
                /*
                 * Evaluate with deterministic policy groups (not periodic) EAGER version (case 7)
                 */

				StreamExecutionEnvironment env7 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source3 = env7.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> randomWalkPolicyGroups = makeRandomWalkPolicyGroups(randomScenarios.get(i));
				SumAggregation.applyOn(source3, new Tuple3<>(randomWalkPolicyGroups, new LinkedList<>(),
								new LinkedList<>()), AggregationFramework.AGGREGATION_STRATEGY.EAGER,
						AggregationFramework.DISCRETIZATION_TYPE.B2B)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env7.execute();

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;

			if (RUN_GENERAL_NOT_PERIODIC_EAGER && randomScenarios.get(i) != null && randomScenarios.get(i).size() > 0) {
                /*
                 *Evaluate not deterministic version  (case 8)
                 */

				StreamExecutionEnvironment env4 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source4 = env4.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				SumAggregation.applyOn(source4, new Tuple3<>(new LinkedList<>(), makeNDRandomWalkTrigger(randomScenarios.get(i)),
								makeNDRandomWalkEviction(randomScenarios.get(i))), AggregationFramework.AGGREGATION_STRATEGY.EAGER,
						AggregationFramework.DISCRETIZATION_TYPE.B2B)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env4.execute();

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

			testCase++;

			if (RUN_PERIODIC_NO_PREAGG_EAGER) {
                /*
                 *Evaluate periodic setup, but without any pre-aggregation (case 9)
                 */

				StreamExecutionEnvironment env5 = StreamExecutionEnvironment.createLocalEnvironment(1);
				DataStream<Tuple3<Double, Double, Long>> source4 = env5.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

				SumAggregation.applyOn(source4, new Tuple3<>(new LinkedList<>(), makeNDPeriodicTrigger(scenarios.get(i)),
								makeNDPeriodicEviction(scenarios.get(i))), AggregationFramework.AGGREGATION_STRATEGY.EAGER,
						AggregationFramework.DISCRETIZATION_TYPE.B2B)
						.map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

				result = env5.execute();

				setupExperiment(stats, resultWriter, result, i, testCase);
			}

		}
	}

	@Override
	public int getStartCount() {
		return 0;
	}


	@SuppressWarnings("unchecked")
	List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> makePeriodicPolicyGroups(List<Tuple3<String, Double, Double>> settings) {

		List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> policyGroups = new LinkedList<>();
		for (Tuple3<String, Double, Double> setting : settings) {
			DeterministicTriggerPolicy<Tuple3<Double, Double, Long>> trigger;
			DeterministicEvictionPolicy<Tuple3<Double, Double, Long>> eviction;
			if (setting.f0.equals("COUNT")) {
				trigger = new DeterministicCountTriggerPolicy<>(setting.f2.intValue());
				eviction = new DeterministicCountEvictionPolicy<>(setting.f1.intValue());
				policyGroups.add(new DeterministicPolicyGroup<>(trigger, eviction, getCountExtractor()));
			} else {
				trigger = new DeterministicTimeTriggerPolicy<>(setting.f2.intValue(), getTimeWrapper());
				eviction = new DeterministicTimeEvictionPolicy<>(setting.f1.intValue(), getTimeWrapper());
				policyGroups.add(new DeterministicPolicyGroup<>(trigger, eviction, getTimeExtractor()));
			}
		}

		return policyGroups;
	}

	@SuppressWarnings("unchecked")
	List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> makeRandomWalkPolicyGroups(List<Tuple3<String, Double, Double[]>> settings) {

		List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> policyGroups = new LinkedList<>();
		for (Tuple3<String, Double, Double[]> setting : settings) {
			DeterministicTriggerPolicy<Tuple3<Double, Double, Long>> trigger;
			DeterministicEvictionPolicy<Tuple3<Double, Double, Long>> eviction;
			if (setting.f0.equals("COUNT")) {
				trigger = new DeterministicRandomWalkTriggerPolicy<>(setting.f2, getCountExtractor());
				eviction = new DeterministicCountEvictionPolicy<>(setting.f1.intValue());
				policyGroups.add(new DeterministicPolicyGroup<>(trigger, eviction, getCountExtractor()));
			} else {
				trigger = new DeterministicRandomWalkTriggerPolicy<>(setting.f2, getTimeExtractor());
				eviction = new DeterministicTimeEvictionPolicy<>(setting.f1.intValue(), getTimeWrapper());
				policyGroups.add(new DeterministicPolicyGroup<>(trigger, eviction, getTimeExtractor()));
			}
		}

		return policyGroups;
	}

	List<TriggerPolicy<Tuple3<Double, Double, Long>>> makeNDRandomWalkTrigger(List<Tuple3<String, Double, Double[]>> settings) {
		List<TriggerPolicy<Tuple3<Double, Double, Long>>> trigger = new LinkedList<>();
		for (Tuple3<String, Double, Double[]> setting : settings) {
			if (setting.f0.equals("COUNT")) {
				trigger.add(new RandomWalkTriggerPolicy<>(setting.f2, getCountExtractor()));
			} else {
				trigger.add(new RandomWalkTriggerPolicy<>(setting.f2, getTimeExtractor()));
			}
		}
		return trigger;
	}

	@SuppressWarnings("unchecked")
	List<EvictionPolicy<Tuple3<Double, Double, Long>>> makeNDRandomWalkEviction(List<Tuple3<String, Double, Double[]>> settings) {
		List<EvictionPolicy<Tuple3<Double, Double, Long>>> eviction = new LinkedList<>();
		for (Tuple3<String, Double, Double[]> setting : settings) {
			if (setting.f0.equals("COUNT")) {
				eviction.add(new CountEvictionPolicy<>(setting.f1.intValue()));
			} else {
				eviction.add(new TimeEvictionPolicy<>(setting.f1.intValue(), getTimeWrapper()));
			}
		}
		return eviction;
	}

	@SuppressWarnings("unchecked")
	List<TriggerPolicy<Tuple3<Double, Double, Long>>> makeNDPeriodicTrigger(List<Tuple3<String, Double, Double>> settings) {
		List<TriggerPolicy<Tuple3<Double, Double, Long>>> trigger = new LinkedList<>();
		for (Tuple3<String, Double, Double> setting : settings) {
			if (setting.f0.equals("COUNT")) {
				trigger.add(new CountTriggerPolicy<>(setting.f2.intValue()));
			} else {
				trigger.add(new TimeTriggerPolicy<>(setting.f2.intValue(), getTimeWrapper()));
			}
		}
		return trigger;
	}

	@SuppressWarnings("unchecked")
	List<EvictionPolicy<Tuple3<Double, Double, Long>>> makeNDPeriodicEviction(List<Tuple3<String, Double, Double>> settings) {
		List<EvictionPolicy<Tuple3<Double, Double, Long>>> eviction = new LinkedList<>();
		for (Tuple3<String, Double, Double> setting : settings) {
			if (setting.f0.equals("COUNT")) {
				eviction.add(new DeterministicCountEvictionPolicy<>(setting.f1.intValue()));
			} else {
				eviction.add(new DeterministicTimeEvictionPolicy<>(setting.f1.intValue(), getTimeWrapper()));
			}
		}
		return eviction;
	}


	/**
	 * The data source for all the experiments
	 */
	public static class DataGenerator implements
			SourceFunction<Tuple3<Double, Double, Long>> {

		private static final long serialVersionUID = 1L;

		volatile boolean isRunning = false;
		Random rnd;
		int sleepMillis;
		int numTuples;
		double c = 1;
		long time = 0;

		public DataGenerator(int sleepMillis, int numTuples) {
			this.sleepMillis = sleepMillis;
			this.numTuples = numTuples;
		}

		@Override
		public void run(SourceContext<Tuple3<Double, Double, Long>> ctx)
				throws Exception {
			isRunning = true;
			rnd = new Random();
			int i = 0;
			while (isRunning && i++ < numTuples) {
				ctx.collect(new Tuple3<>((double) rnd.nextInt(1000), c++, time++));
				Thread.sleep(sleepMillis);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

	}

	/**
	 * Main program: Runs all the test cases and writed the results to the specified output files.
	 *
	 * @param args not used,
	 * @throws Exception Any exception which may occurs at the runtime.
	 */
	public static void main(String[] args) throws Exception {

		SimExperimentDriver experiment = null;
		//If setup and result paths are provided as paramaters, replace the defaults
		if (args.length == 2) {
			experiment = new SimExperimentDriver(args[0], args[1]);
		}
		experiment.execute();
	}

	public static AggregationFramework.WindowAggregation<Double, Tuple3<Double, Double, Long>, Double>
			SumAggregation = new AggregationFramework.WindowAggregation<>(
			new MapFunction<Double, Double>() {
				@Override
				public Double map(Double t) throws Exception {
					return t;
				}
			}, new ReduceFunction<Double>() {
		private static final long serialVersionUID = 1L;
		private AggregationStats stats = AggregationStats.getInstance();

		@Override
		public Double reduce(Double value1, Double value2)
				throws Exception {
			stats.registerReduce();
			return value1 + value2;
		}
	}, new MapFunction<Tuple3<Double, Double, Long>, Double>() {
		@Override
		public Double map(Tuple3<Double, Double, Long> value) throws Exception {
			return value.f0;
		}
	}, 0d);

}
