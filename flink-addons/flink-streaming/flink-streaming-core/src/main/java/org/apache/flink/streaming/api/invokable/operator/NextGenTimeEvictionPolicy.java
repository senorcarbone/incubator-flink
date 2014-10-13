package org.apache.flink.streaming.api.invokable.operator;

import java.util.LinkedList;

import org.apache.flink.streaming.api.invokable.util.TimeStamp;

public class NextGenTimeEvictionPolicy <DATA> implements NextGenEvictionPolicy<DATA>{

	/**
	 * auto generated version id
	 */
	private static final long serialVersionUID = -1457476766124518220L;

	private long granularity;
	private TimeStamp<DATA> timestamp;
	private LinkedList<DATA> buffer;
	
	public NextGenTimeEvictionPolicy(long granularity, TimeStamp<DATA> timestamp) {
		this.timestamp=timestamp;
		this.granularity=granularity;
	}
	
	@Override
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize) {
		//check for deleted tuples (deletes by other policies)
		while (bufferSize>this.buffer.size()){
			this.buffer.removeFirst();
		}
		
		//delete and count expired tuples
		int counter=0;
		for (DATA d:buffer){
			if (timestamp.getTimestamp(d)<timestamp.getTimestamp(datapoint)-granularity){
				buffer.removeFirst();
			}
		}
		
		//return result
		return counter;
	}

}
