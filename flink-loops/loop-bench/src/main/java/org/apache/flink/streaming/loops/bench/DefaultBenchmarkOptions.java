package org.apache.flink.streaming.loops.bench;

import java.util.LinkedList;
import java.util.List;

public class DefaultBenchmarkOptions {

	public static final List<BenchmarkOption<?>> configs = new LinkedList<>();

	static {
		configs.add(new BenchmarkOption<>("bench.id", "default"));
		configs.add(new BenchmarkOption<>("bench.algorithm", "StreamingPageRank"));
		configs.add(new BenchmarkOption<>("bench.dataset", "ldbc"));
		configs.add(new BenchmarkOption<>("bench.parallelism", 4));
		configs.add(new BenchmarkOption<>("window.count", 0));
		configs.add(new BenchmarkOption<>("window.size", 1000l));
		configs.add(new BenchmarkOption<>("iteration.count", 4));
		configs.add(new BenchmarkOption<>("iteration.latency_ps", true));
		configs.add(new BenchmarkOption<>("iteration.sync_overhead_ps", true));
		configs.add(new BenchmarkOption<>("iteration.active_keys_ps", false));
	}
}
