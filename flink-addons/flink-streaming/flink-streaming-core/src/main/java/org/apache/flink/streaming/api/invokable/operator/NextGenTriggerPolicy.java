package org.apache.flink.streaming.api.invokable.operator;

import java.io.Serializable;

public interface NextGenTriggerPolicy<DATA> extends Serializable {

    /**
     * Proves and returns if a new window should be started.
     * In case the trigger occurs (return value true) the UDF will
     * be executed on the current element buffer without the last
     * added element which is provided as parameter. This element will
     * be added to the buffer after the execution of the UDF.
     * 
     * There are possibly different strategies for eviction and triggering:
     *   1) including last data point: Better/faster for count eviction
     *   2) excluding last data point: Essentially required for time based eviction and delta rules
     * As 2) is required for some policies and the benefit of using 1) is
     * small for the others, policies are implemented according to 2).
     *
     * @param datapoint the data point which arrived
     * @return true if the current windows should be closed, otherwise false.
     * In true case the given data point will be part of the
     * next window and will not be included in the current one.
     */
    public boolean notifyTrigger(DATA datapoint);


}
