package org.apache.flink.streaming.api.invokable.operator;

public class NextGenCountEvictionPolicy<IN> implements NextGenPolicy<IN> {

    /**
     * Auto generated version ID
     */
    private static final long serialVersionUID = -6357200688886103968L;

    private static final int DEFAULT_START_VALUE = 0;

    private int counter;
    private int max;

    public NextGenCountEvictionPolicy(int max) {
        this(max, DEFAULT_START_VALUE);
    }

    public NextGenCountEvictionPolicy(int max, int startValue) {
        this.max = max;
        this.counter = startValue;
    }

    @Override
    public boolean addDataPoint(IN datapoint) {
        if (counter == max) {
            //The current data point will be part of the next window!
            //Therefore counter needs to be set to one already.
            //TODO think about introducing different strategies for eviction:
            //     1) including last data point: Better/faster for count eviction
            //     2) excluding last data point: Essentially required for time based eviction and delta rules
            counter = 1;
            return true;
        } else {
            counter++;
            return false;
        }
    }


}
