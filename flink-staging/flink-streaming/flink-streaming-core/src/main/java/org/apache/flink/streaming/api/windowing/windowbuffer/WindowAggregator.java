package org.apache.flink.streaming.api.windowing.windowbuffer;


public interface WindowAggregator<T> {

    /**
     * It adds a new node in the aggregation buffer with the given id and value
     * @param id
     * @param val
     * @throws Exception
     */
    public void add(int id, T val) throws Exception;

    /**
     * It evicts the elements with the given ids. Currently only FIFO evictions are possible so a discretizer
     * should always invoke removals consecutively from id==HEAD.
     * 
     * @param ids
     * @throws Exception
     */
    public void remove(Integer... ids) throws Exception;

    /**
     * Returns the aggregate of the window buffer from startid to the front of the buffer
     * @param startid
     * @return
     */
    public T aggregate(int startid) throws Exception;

    /**
     * Returns a full aggregate for the whole window buffer
     * @return
     */
    public T aggregate() throws Exception;
         
}
