package org.apache.flink.streaming.api.windowing.windowbuffer;

import java.io.Serializable;

public class AggregationStats implements Serializable{

    private static AggregationStats ourInstance = new AggregationStats();

    private long update_count = 0l;
    private long aggregate_count = 0l;
    private long reduce_count = 0l;

    public static AggregationStats getInstance() {
        return ourInstance;
    }

    private AggregationStats() {
    }

    public void registerUpdate() {
        update_count++;
    }

    public void registerAggregate() {
        aggregate_count++;
    }

    public void registerReduce() {
        reduce_count++;
    }

    public long getUpdateCount() {
        return update_count;
    }

    public long getAggregateCount() {
        return aggregate_count;
    }

    public long getReduceCount() {
        return reduce_count;
    }
}
