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
     * It evicts the element with the given id. Currently only FIFO evictions are possible 
     * 
     * @param id
     * @throws Exception
     */
    public void remove(int id) throws Exception;

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
