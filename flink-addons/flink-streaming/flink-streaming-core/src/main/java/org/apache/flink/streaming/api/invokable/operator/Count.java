package org.apache.flink.streaming.api.invokable.operator;

public class Count<DATA> implements NextGenWindowHelper<DATA> {

	private int count;

	public Count(int count) {
		this.count = count;
	}

	@Override
	public NextGenEvictionPolicy<DATA> toEvict() {
		return new NextGenCountEvictionPolicy<DATA>(count);
	}

	@Override
	public NextGenTriggerPolicy<DATA> toTrigger() {
		return new NextGenCountTriggerPolicy<DATA>(count);
	}

	public static Count of(int count) {
		return new Count(count);
	}

}
