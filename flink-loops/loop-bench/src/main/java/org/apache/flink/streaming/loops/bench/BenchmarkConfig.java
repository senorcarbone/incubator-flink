package org.apache.flink.streaming.loops.bench;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class BenchmarkConfig<T> {
	public static final String DEFAULT_CONF_FILE = "bench_config.yml";
	private HashMap<String, BenchmarkOption<?>> configs = new HashMap<>();

	public BenchmarkConfig(String confFile) {
		if (!new File(getClass().getClassLoader().getResource(confFile).getFile()).exists()) throw new IllegalArgumentException("Configuration file "+confFile+" could not be found");
		loadDefaults();
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(confFile);
		Yaml yaml = new Yaml();
		Map<String, Object> values = yaml.load(inputStream);

		for (Map.Entry<String, Object> mp : values.entrySet()) {
			BenchmarkOption<?> b = this.configs.get(mp.getKey());
			if (b != null) {
				String value = mp.getValue().toString();
				OptionParser<?> op = new OptionParser<>(b);
				this.configs.put(mp.getKey(), new BenchmarkOption<>(mp.getKey(), op.parse(value)));
			}
			else throw new IllegalArgumentException("Configuration parameter "+mp.getKey()+" could not be found in default BenchmarkOptions");
		}
	}

	public BenchmarkConfig() {
		this(DEFAULT_CONF_FILE);
	}

	public T getParam(String key) {
		if (this.configs.get(key) == null) { 
			throw new IllegalArgumentException("Parameter "+key+" not set in the benchmark configuration");
		}
		return (T) this.configs.get(key).getValue();
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
}
