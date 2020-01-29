package org.apache.flink.streaming.examples.iteration.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class BenchmarkConfig<T> {
	private HashMap<String, BenchmarkOption<?>> configs = new HashMap<>();

	public BenchmarkConfig() {
		loadDefaults();

		InputStream inputStream = this.getClass().getResourceAsStream("/bench_config.yml");
		Yaml yaml = new Yaml();
		Map<String, Object> values = yaml.load(inputStream);

		for (Map.Entry<String, Object> mp : values.entrySet()) {
			BenchmarkOption<?> b = this.configs.get(mp.getKey());
			if (b != null) {
				String value = mp.getValue().toString();
				OptionParser<?> op = new OptionParser<>(b);
				this.configs.put(mp.getKey(), new BenchmarkOption<>(mp.getKey(), op.parse(value)));
			}
		}
	}

	public T getParam(String key) {
		if (this.configs.get(key) != null) {
			return (T) this.configs.get(key).getValue();
		}
		return null;
	}

	public void printConfiguration() {
		for (Map.Entry<String, BenchmarkOption<?>> e : configs.entrySet()) {
			System.out.println(e.getKey() + " || " + e.getValue());
		}
	}

	private void loadDefaults() {
		for (BenchmarkOption<?> t : DefaultBenchmarkOptions.configs) {
			this.configs.put(t.getKey(), t);
		}
	}

//	public static void main(String[] args) {
//		BenchmarkConfig benchmarkConfig = new BenchmarkConfig();
//		benchmarkConfig.printConfiguration();
//		System.out.println(benchmarkConfig.getParam("window.ssize")); // typo was intentional
//		System.out.println(benchmarkConfig.getParam("window.size"));
//		System.out.println(benchmarkConfig.getParam("job.parallelism"));
//		System.out.println(benchmarkConfig.getParam("dataset"));
//		System.out.println(benchmarkConfig.getParam("toggle.latency_ps"));
//		System.out.println(benchmarkConfig.getParam("toggle.sync_overhead_ps"));
//		System.out.println(benchmarkConfig.getParam("toggle.n_active_keys_ps"));
//	}
}
