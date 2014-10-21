package org.apache.flink.streaming.api.windowing.policy;

/**
 * In case an {@link ActiveTriggerPolicy} is used, it can implement own
 * {@link Runnable} classes. Such {@link Runnable} classes will be executed as
 * an own thread and can submit fake elements, to the element
 * buffer at any time.
 * 
 * The factory method for runnables of the {@link ActiveTriggerPolicy} gets an
 * instance of this interface as parameter. The describes adding of elements can
 * be done by the runnable using the methods provided in this interface.
 * 
 * @param <DATA>
 *            The data type which can be consumed by the methods provided in
 *            this callback implementation.
 */
public interface ActiveTriggerCallback<DATA> {

	/**
	 * Submits a new fake data point to the element buffer. Such a fake element
	 * might be used to trigger at any time, but will never be included in the
	 * result of the reduce function. The submission of a fake element causes
	 * notifications only at the {@link ActiveTriggerPolicy} and
	 * {@link ActiveEvictionPolicy} implementations.
	 * 
	 * @param datapoint
	 *            the fake data point to be added
	 */
	public void sendFakeElement(DATA datapoint);

}
