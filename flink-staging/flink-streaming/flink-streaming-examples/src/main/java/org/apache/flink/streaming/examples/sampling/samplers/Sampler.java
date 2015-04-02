package org.apache.flink.streaming.examples.sampling.samplers;


public interface Sampler<T> {

	/**
	 * @return an array holding the whole sample
	 */
	public T[] getElements();

	/**
	 * In a streaming fashion a sampler receives individual elements and puts them in its sample.
	 * The sample method should include the core sample creation logic of a stream sampler. 
	 * @param element
	 */
	public void sample(T element);

	/**
	 * 
	 * @return The current size of the sample
	 */
	public int size();
	
	
}
