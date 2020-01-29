package org.apache.flink.streaming.examples.iteration.config;

import java.util.LinkedList;
import java.util.List;

public class DefaultBenchmarkOptions {
	private static final BenchmarkOption<Integer> WINDOW_COUNT = new BenchmarkOption<>("window.count", 0);

	private static final BenchmarkOption<Integer> WINDOW_SIZE = new BenchmarkOption<>("window.size", 0);

	private static final BenchmarkOption<Integer> JOB_PARALLELISM = new BenchmarkOption<>("job.parallelism", 4);

	private static final BenchmarkOption<String> DIR_INPUT = new BenchmarkOption<>("dir.input", "");

	private static final BenchmarkOption<String> DIR_OUTPUT = new BenchmarkOption<>("dir.output", "");

	private static final BenchmarkOption<String> DATASET = new BenchmarkOption<>("dataset", "");

	private static final BenchmarkOption<Boolean> TOGGLE_LATENCY_PS = new BenchmarkOption<>("toggle.latency_ps", false);

	private static final BenchmarkOption<Boolean> TOGGLE_SYNC_OVERHEAD_PS = new BenchmarkOption<>("toggle.sync_overhead_ps", false);

	private static final BenchmarkOption<Boolean> TOGGLE_N_ACTIVE_KEYS_PS = new BenchmarkOption<>("toggle.n_active_keys_ps", false);

	private static final BenchmarkOption<Integer> WINDOW_N_ITERATIONS = new BenchmarkOption<>("window.n_iterations", 4);

	public static final List<BenchmarkOption<?>> configs = new LinkedList<>();

	static {
		configs.add(WINDOW_SIZE);
		configs.add(WINDOW_COUNT);
		configs.add(WINDOW_N_ITERATIONS);
		configs.add(JOB_PARALLELISM);
		configs.add(DIR_INPUT);
		configs.add(DIR_OUTPUT);
		configs.add(DATASET);
		configs.add(TOGGLE_LATENCY_PS);
		configs.add(TOGGLE_SYNC_OVERHEAD_PS);
		configs.add(TOGGLE_N_ACTIVE_KEYS_PS);
	}
}
