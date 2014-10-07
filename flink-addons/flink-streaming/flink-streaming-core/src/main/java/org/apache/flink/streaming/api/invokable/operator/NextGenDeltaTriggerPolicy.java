package org.apache.flink.streaming.api.invokable.operator;


public class NextGenDeltaTriggerPolicy<DATA> implements NextGenTriggerPolicy<DATA> {

    private NextGenDeltaPredicate<T> predicate;

    public NextGenDeltaTriggerPolicy<DATA>(NextGenDeltaPredicate<T> predicate)
    {
        this.predicate = predicate;
    }

    @Override
    public boolean addDataPoint(DATA datapoint) {

    }
}
