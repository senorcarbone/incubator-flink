package org.apache.flink.streaming.api.invokable.operator;

import java.io.Serializable;

public interface NextGenPolicy<DATA> extends Serializable {

    /**
     * Proves and returns if a new window should be started
     *
     * @param datapoint the data point in the stream which arrived
     * @return true if the current windows should be closed, otherwise false.
     * In true case the given data point should be part	of the
     * next window and should not be included in the current one.
     */
    public boolean addDataPoint(DATA datapoint);


}
