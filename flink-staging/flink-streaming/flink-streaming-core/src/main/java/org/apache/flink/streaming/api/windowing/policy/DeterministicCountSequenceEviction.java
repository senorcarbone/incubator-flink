package org.apache.flink.streaming.api.windowing.policy;


import java.util.List;

/**
 * As the name hints this is a deterministic count-based eviction policy that cycles through a given sequence of count
 * sequences. This can be particularly useful for certain pre-computed types of pre-aggregations such as pairs.
 *  
 * @param <IN>
 */
public class DeterministicCountSequenceEviction<IN> implements CloneableEvictionPolicy<IN>, DeterministicEvictionPolicy<IN> {

    private final List<Long> sequence;
    private int curIndex;
    private long counter;

    public DeterministicCountSequenceEviction(List<Long> sequence) {
        this.sequence = sequence;
        this.curIndex = 0;
    }

    @Override
    public double getLowerBorder(double upperBorder) {
        if (upperBorder - getCurrentMax() > 0) {
            return upperBorder - getCurrentMax();
        } else {
            return 0;
        }
    }

    private Long getCurrentMax() {
        return sequence.get(curIndex);
    }

    @Override
    public CloneableEvictionPolicy<IN> clone() {
        return new DeterministicCountSequenceEviction<IN>(sequence);
    }

    @Override
    public int notifyEviction(IN datapoint, boolean triggered, int bufferSize) {
        int toEvict;
        if (counter >= getCurrentMax()) {
            counter = (counter != 0) ? counter - 1 : 0;
            counter++;
            toEvict = 1;
        } else {
            counter++;
            toEvict = 0;
        }
        if (triggered) {
            proceedMax();
        }
        return toEvict;
    }

    private void proceedMax() {
        curIndex = (curIndex + 1) % sequence.size();
    }
}
