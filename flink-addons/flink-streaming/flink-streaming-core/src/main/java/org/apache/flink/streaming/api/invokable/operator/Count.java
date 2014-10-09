package org.apache.flink.streaming.api.invokable.operator;

@SuppressWarnings("rawtypes")
public class Count implements NextGenWindowHelper {

	private int count;

	public Count(int count) {
		this.count = count;
	}

	@Override
	public NextGenEvictionPolicy<?> toEvict() {
		return new NextGenCountEvictionPolicy(count);
	}

	@Override
	public NextGenTriggerPolicy<?> toTrigger() {
		return new NextGenCountTriggerPolicy(count);
	}

	public static Count of(int count) {
		return new Count(count);
	}

}
