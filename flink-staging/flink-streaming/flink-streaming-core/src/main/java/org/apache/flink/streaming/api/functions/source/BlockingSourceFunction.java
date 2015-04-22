package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.util.Collector;


public abstract class BlockingSourceFunction<T> extends RichParallelSourceFunction<T>{

	public abstract void toggleBlock();
	
}
