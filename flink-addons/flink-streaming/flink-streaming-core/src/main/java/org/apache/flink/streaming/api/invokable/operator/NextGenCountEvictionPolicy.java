package org.apache.flink.streaming.api.invokable.operator;

/**
 * This eviction policy allows the eviction of data points from the buffer using
 * a counter of arriving elements and a threshold (maximal buffer size)
 * @param <IN>
 */
public class NextGenCountEvictionPolicy<IN> implements NextGenEvictionPolicy<IN> {

	/**
	 * Auto generated version id
	 */
	private static final long serialVersionUID = 2319201348806427996L;
	
	int maxElements;
	int counter=0;
	int deleteOnEviction=1;
	
	/**
	 * This constructor allows the setup of the simplest possible count
	 * based eviction.
	 * It keeps the size of the buffer according to the
	 * given maxElements parameter by deleting the oldest element in the
	 * buffer. Eviction only takes place if the counter of arriving elements
	 * would be higher than maxElements without eviction.  
	 * @param maxElements The maximum number of elements before eviction.
	 *        As soon as one more element arrives, the oldest element
	 *        will be deleted 
	 */
	public NextGenCountEvictionPolicy(int maxElements) {
		this.maxElements=maxElements;
	}
	
	/**
	 * This constructor allows to set up both, the maximum number of elements
	 * and the number of elements to be deleted in case of an eviction.
	 * 
	 * Eviction only takes place if the counter of arriving elements
	 * would be higher than maxElements without eviction. In such a case
	 * deleteOnEviction elements will be removed from the buffer.
	 * 
	 * The counter of arriving elements is adjusted respectively, but never
	 * set below zero:
	 *     counter=(counter-deleteOnEviction<0)?0:counter-deleteOnEviction
	 * 
	 * @param maxElements maxElements The maximum number of elements before eviction.
	 * @param deleteOnEviction The number of elements to be deleted on eviction.
	 *                         The counter will be adjusted respectively but never
	 *                         below zero.
	 */
	public NextGenCountEvictionPolicy(int maxElements, int deleteOnEviction){
		this(maxElements);
		this.deleteOnEviction=deleteOnEviction;
	}
	
	/**
	 * The same as {@link NextGenCountEvictionPolicy#NextGenCountEvictionPolicy(int, int)}.
	 * Additionally a custom start value for the counter of arriving elements can be set.
	 * @param maxElements maxElements The maximum number of elements before eviction.
	 * @param deleteOnEviction The number of elements to be deleted on eviction.
	 *                         The counter will be adjusted respectively but never
	 *                         below zero.
	 * @param startValue A custom start value for the counter of arriving elements.
	 * @see NextGenCountEvictionPolicy#NextGenCountEvictionPolicy(int, int)
	 */
	public NextGenCountEvictionPolicy(int maxElements, int deleteOnEviction, int startValue){
		this(maxElements,deleteOnEviction);
		this.counter=startValue;
	}
	
	@Override
	public int notifyEviction(IN datapoint, boolean triggered) {
		if (counter==maxElements){
			//Adjust the counter according to the current eviction
			counter=(counter-deleteOnEviction<0)?0:counter-deleteOnEviction;
			//The current element will be added after the eviction
			//Therefore, increase counter in any case
			counter++;
			return deleteOnEviction;
		} else {
			counter++;
			return 0;
		}
	}
	
}
