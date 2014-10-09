package org.apache.flink.streaming.api.invokable.operator;

/**
 * This eviction policy deletes all elements from the buffer
 * in case a trigger occurred. Therefore, it is the default
 * eviction policy to be used for any tumbling window.
 * @param <DATA> The type of the data points which is handled by this policy
 */
public class NextGenTumblingEvictionPolicy<DATA> implements
		NextGenEvictionPolicy<DATA> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = -4018019069267281155L;
	
	/**
	 * Counter for the current number of elements in the buffer
	 */
	private int counter=0;
	
	/**
	 * This is the default constructor providing no special functionality
	 */
	public NextGenTumblingEvictionPolicy() {
		// default constructor, no further logic needed
	}
	
	/**
	 * This constructor allows to set a custom start value for the element counter
	 * @param startValue A start value for the element counter
	 */
	public NextGenTumblingEvictionPolicy(int startValue){
		this.counter=startValue;
	}

	/**
	 * Deletes all elements from the buffer in case the trigger occurred.
	 */
	@Override
	public int notifyEviction(Object datapoint, boolean triggered) {
		if (triggered) {
            //The current data point will be part of the next window!
            //Therefore the counter needs to be set to one already.
            int tmpCounter=counter;
			counter = 1;
            return tmpCounter;
        } else {
            counter++;
            return 0;
        }
	}
}
