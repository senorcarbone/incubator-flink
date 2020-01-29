package org.apache.flink.streaming.util;

public class IterationsTimer {
	private String timerName;
	private Long start;
	private Long end;

	public IterationsTimer(String timerName) {
		this.start = System.currentTimeMillis();
		this.timerName = timerName;
	}

	public void stop() {
		this.end = System.currentTimeMillis();
	}

	public String toString() {
		if (this.end != null) {
			return String.format("%s,%d ms", this.timerName, this.end - this.start);
		}
		return "";
	}
}
