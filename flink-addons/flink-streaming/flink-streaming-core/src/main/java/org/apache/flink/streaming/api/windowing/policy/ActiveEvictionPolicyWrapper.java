package org.apache.flink.streaming.api.windowing.policy;

/**
 * This {@link ActiveEvictionPolicy} wraps around a non active
 * {@link EvictionPolicy}. It forwards all calls to
 * {@link ActiveEvictionPolicy#notifyEvictionWithFakeElement(Object, int)} to
 * {@link EvictionPolicy#notifyEviction(Object, boolean, int)} while the
 * triggered parameter will be set to true.
 * 
 * @param <DATA>
 *            The data type handled by this policy
 */
public class ActiveEvictionPolicyWrapper<DATA> implements ActiveEvictionPolicy<DATA> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = -7656558669799505882L;
	private EvictionPolicy<DATA> nestedPolicy;

	/**
	 * Creates a wrapper which activates the eviction policy which is wrapped
	 * in. This means that the nested policy will get called on fake elements as
	 * well as on real elements.
	 * 
	 * @param nestedPolicy
	 *            The policy which should be activated/wrapped in.
	 */
	public ActiveEvictionPolicyWrapper(EvictionPolicy<DATA> nestedPolicy) {
		if (nestedPolicy == null) {
			throw new RuntimeException("The nested policy must not be null.");
		}
		this.nestedPolicy = nestedPolicy;
	}

	@Override
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize) {
		return nestedPolicy.notifyEviction(datapoint, triggered, bufferSize);
	}

	@Override
	public int notifyEvictionWithFakeElement(DATA datapoint, int bufferSize) {
		return nestedPolicy.notifyEviction(datapoint, true, bufferSize);
	}

}
