package org.apache.flink.streaming.examples.iteration.config;

public class OptionParser<T> {
	BenchmarkOption<T> bo;

	public OptionParser(BenchmarkOption<T> bo) {
		this.bo = bo;
	}

	T parse(String value) {
		if (bo.getType() == Integer.class) {
			return (T) new Integer(value);
		} else if (bo.getType() == Long.class) {
			return (T) new Long(value);
		} else if (bo.getType() == Double.class) {
			return (T) new Double(value);
		} else if (bo.getType() == Boolean.class) {
			return (T) new Boolean(value);
		} else {
			return (T) value;
		}
	}
}
