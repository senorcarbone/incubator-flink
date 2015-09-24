package org.apache.flink.streaming.api.windowing.windowbuffer;

import java.io.Serializable;

public class AggregationStats implements Serializable {

    private static AggregationStats ourInstance;

    private long update_count = 0l;
    private long aggregate_count = 0l;
    private long reduce_count = 0l;
    private long max_buf_size = 0l;
    private long sum_buf_size = 0l;
    private long cnt_buf_size = 0l;
    
    private long totalUpdateCount = 1l;
    private long sum_upd_time = 0l;
    private long totalMergeCount = 1l;
    private long sum_merge_time = 0l;
    
    private long upd_timestamp;
    private long merge_timestamp;

    public static AggregationStats getInstance() {
        if (ourInstance == null) {
            return ourInstance = new AggregationStats();
        } else {
            return ourInstance;
        }
    }

    private AggregationStats() {
    }

    public void registerUpdate() {
        update_count++;
    }
    
    public void registerStartUpdate(){
        this.upd_timestamp = System.currentTimeMillis();
    }
    
    public void registerEndUpdate(){
        this.totalUpdateCount++;
        this.sum_upd_time += System.currentTimeMillis()-upd_timestamp;
    }

    public void registerStartMerge(){
        this.merge_timestamp = System.currentTimeMillis();
    }

    public void registerEndMerge(){
        this.totalMergeCount++;
        this.sum_merge_time += System.currentTimeMillis()-merge_timestamp;
    }

    public void registerAggregate() {
        aggregate_count++;
    }

    public void registerReduce() {
        reduce_count++;
    }

    public void registerBufferSize(int bufSize) {
        cnt_buf_size++;
        sum_buf_size += bufSize;
        max_buf_size = max_buf_size < bufSize ? bufSize : max_buf_size;
    }

    public double getAverageBufferSize() {
        return ((double) sum_buf_size) / cnt_buf_size;
    }

    public long getMaxBufferSize() {
        return max_buf_size;
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

    public void reset() {
        update_count = 0l;
        aggregate_count = 0l;
        reduce_count = 0l;
        max_buf_size = 0l;
        sum_buf_size = 0l;
        cnt_buf_size = 0l;
        totalUpdateCount = 1l;
        sum_upd_time = 0l;
        totalMergeCount = 1l;
        sum_merge_time = 0l;
        upd_timestamp = 0l;
        merge_timestamp = 0l;
    }

    protected Object readResolve() {
        return getInstance();
    }

    public long getTotalMergeCount() {
        return totalMergeCount;
    }

    public long getTotalUpdateCount() {
        return totalUpdateCount;
    }

    public double getAverageMergeTime() {
        return (double) sum_merge_time/ totalMergeCount;
    }
    
    public double getAverageUpdTime(){
        return (double) sum_upd_time/ totalUpdateCount;
    }
}
