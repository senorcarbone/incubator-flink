package org.apache.flink.streaming.api.windowing.windowbuffer;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;

public class AggregationStats implements Serializable{

    private static AggregationStats ourInstance;

    private long update_count = 0l;
    private long aggregate_count = 0l;
    private long reduce_count = 0l;

    public static AggregationStats getInstance() {
        if (ourInstance==null) {
            return ourInstance=new AggregationStats();
        } else {
            return ourInstance;
        }
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

    public void reset(){
        update_count = 0l;
        aggregate_count = 0l;
        reduce_count = 0l;
    }

    protected Object readResolve() {
        return getInstance();
    }
}
