package org.apache.flink.streaming.api.windowing.windowbuffer;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class AggregationStats implements Serializable {

	private static AggregationStats ourInstance;


	private transient ThreadMXBean mxbean = ManagementFactory.getThreadMXBean();

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
	private long num_partials = 0l;

	private long upd_timestamp;
	private long merge_timestamp;
	
	private long operatorStartTS;
	private long startCPUTime;
	private int totalOperatorInvokes;
	private long sumOperatorTime;
	private long sumOperatorCPUTime;

	public void startRecord() {
		if(mxbean == null){
			mxbean = ManagementFactory.getThreadMXBean();
		}
		this.operatorStartTS = System.currentTimeMillis();
		this.startCPUTime = mxbean.getCurrentThreadCpuTime();

	}

	public void endRecord() {
		totalOperatorInvokes++;
		sumOperatorCPUTime += mxbean.getCurrentThreadCpuTime() - this.startCPUTime;
		sumOperatorTime += System.currentTimeMillis() - this.operatorStartTS;

	}


	public enum AGGREGATION_MODE {UPDATES, AGGREGATES}

	;

	public AGGREGATION_MODE state = AGGREGATION_MODE.UPDATES;

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

	public void registerStartUpdate() {
		this.upd_timestamp = System.currentTimeMillis();
	}

	public void registerEndUpdate() {
		this.totalUpdateCount++;
		this.sum_upd_time += System.currentTimeMillis() - upd_timestamp;
	}

	public void setAggregationMode(AGGREGATION_MODE mode) {
		this.state = mode;
	}

	public void registerStartMerge() {
		this.merge_timestamp = System.currentTimeMillis();
	}

	public void registerEndMerge() {
		this.totalMergeCount++;
		this.sum_merge_time += System.currentTimeMillis() - merge_timestamp;
	}

	public void registerAggregate() {
		aggregate_count++;
	}

	public void registerPartial() {
		num_partials++;
	}

	public void registerReduce() {
		reduce_count++;
		if (this.state == AGGREGATION_MODE.UPDATES) {
			registerUpdate();
		} else if (this.state == AGGREGATION_MODE.AGGREGATES) {
			registerAggregate();
		}
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

	public long getPartialCount() {
		return num_partials;
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
		num_partials = 0l;
		totalOperatorInvokes = 0;
		sumOperatorCPUTime = 0l;
		sumOperatorTime = 0l;
	}

	public long getSumOperatorCPUTime() {
		return sumOperatorCPUTime;
	}

	public long getSumOperatorTime() {
		return sumOperatorTime;
	}
	
	public double getAvgOperatorTime(){
		return sumOperatorTime / totalOperatorInvokes;
	}

	public double getAvgOperatorCPUTime(){
		return sumOperatorCPUTime / totalOperatorInvokes;
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
		return (double) sum_merge_time / totalMergeCount;
	}

	public double getAverageUpdTime() {
		return (double) sum_upd_time / totalUpdateCount;
	}
}
