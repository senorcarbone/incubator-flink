package org.apache.flink.streaming.api.invokable.operator;

public class NextGenDeltaPolicy<DATA> implements NextGenTriggerPolicy<DATA>, NextGenEvictionPolicy<DATA> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = -7797538922123394967L;
	
	private static final int DEFAULT_DELETE_ON_EVICTION=1;
	
	private NextGenDeltaFunction<DATA> deltaFuntion;
	private DATA oldDataPoint;
	private int threshold;
	private int deleteOnEviction;
	
	public NextGenDeltaPolicy(NextGenDeltaFunction<DATA> deltaFuntion,DATA init,int threshold) {
		this.deltaFuntion=deltaFuntion;
		this.oldDataPoint=init;
		this.threshold=threshold;
	}
	
	public NextGenDeltaPolicy(NextGenDeltaFunction<DATA> deltaFuntion,DATA init,int threshold,int deleteOnEviction) {
		this(deltaFuntion,init,threshold);
		this.deleteOnEviction=deleteOnEviction;
	}
	
	@Override
	public boolean addDataPoint(DATA datapoint) {
		if (deltaFuntion.getDelta(this.oldDataPoint, datapoint)>this.threshold){
			//TODO is this true??? Maybe we need one point earlier
			this.oldDataPoint=datapoint;
			return true;
		} else {
			return false;
		}
		
	}

	@Override
	public int addDataPoint(DATA datapoint, boolean triggered) {
		if (addDataPoint(datapoint)){
			return deleteOnEviction;
		} else {
			return 0;
		}
	}

}
