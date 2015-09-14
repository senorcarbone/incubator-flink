package org.apache.flink.streaming.api.operators.windowing;

import org.apache.flink.streaming.api.windowing.WindowUtils;
import org.apache.flink.streaming.api.windowing.policy.*;

/**
 * A wrapper of deterministic policy groups that extracts and maintains pair-related information
 */
public class PairPolicyGroup<DATA> extends DeterministicPolicyGroup<DATA> {

    /**
     * Pairs are computed upon initialization based on the following formula:
     * <p/>
     * part2 = range mod slide
     * part1 = slide - part2
     */
    private long part1, part2;


    public PairPolicyGroup(DeterministicPolicyGroup<DATA> group) {
        super(group.getTrigger(), group.getEviction());

        //Possibly not the cleanest code ever made
        //we only do this in the name of science...

        long range, slide = 0;

        if (group.getEviction() instanceof DeterministicCountEvictionPolicy) {
            range = ((DeterministicCountEvictionPolicy<DATA>) group.getEviction()).getWindowSize();
            slide = ((DeterministicCountTriggerPolicy<DATA>) group.getTrigger()).getSlideSize();
//        } else if (group.getEviction() instanceof DeterministicTimeEvictionPolicy) {
//            range = ((DeterministicTimeEvictionPolicy<DATA>) group.getEviction()).getWindowSize();
//            slide = ((DeterministicTimeTriggerPolicy<DATA>) group.getTrigger()).getSlideSize();
        } else
            throw new IllegalArgumentException("Only time and count based policies are currently supported for pairs ");

        long[] pairs = PairDiscretization.computePairs(range, slide);
        part1 = pairs[0];
        part2 = pairs[1];
    }

    public long getPart1() {
        return part1;
    }

    public long getPart2() {
        return part2;
    }

    /**
     * Just a convenience method for fetching the right part
     *
     * @param indx
     * @return
     */
    public long getPart(int indx) {
        return indx == 0 ? part1 : part2;
    }
}
