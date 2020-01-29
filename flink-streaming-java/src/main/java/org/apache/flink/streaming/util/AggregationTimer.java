package org.apache.flink.streaming.util;

public class AggregationTimer {
	private String name;
	private Long sum = 0L;
	private Long count = 0L;
	private Long max = Long.MIN_VALUE;
	private Long min = Long.MAX_VALUE;

	public AggregationTimer(String name) {
		this.name = name;
	}

	public void addToList(Long value) {
		this.sum += value;
		this.count += 1;
		this.max = Math.max(this.max, value);
		this.min = Math.min(this.min, value);
	}

	public Long getAvg() {
		if (this.count != 0) {
			return this.sum / this.count;
		}
		return 0L;
	}

	public Long getMax() {
		return this.max;
	}

	public Long getMin() {
		return this.min;
	}
}
