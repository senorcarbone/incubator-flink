package org.apache.flink.streaming.api.invokable.operator;

public class NextGenCountTriggerPolicy<IN> implements NextGenTriggerPolicy<IN> {

    /**
     * Auto generated version ID
     */
    private static final long serialVersionUID = -6357200688886103968L;

    private static final int DEFAULT_START_VALUE = 0;

    private int counter;
    private int max;

    public NextGenCountTriggerPolicy(int max) {
        this(max, DEFAULT_START_VALUE);
    }

    public NextGenCountTriggerPolicy(int max, int startValue) {
        this.max = max;
        this.counter = startValue;
    }

    @Override
    public boolean addDataPoint(IN datapoint) {
        if (counter == max) {
            //The current data point will be part of the next window!
            //Therefore the counter needs to be set to one already.
            counter = 1;
            return true;
        } else {
            counter++;
            return false;
        }
    }


}
