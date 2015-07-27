package org.apache.flink.streaming.api.windowing.policy;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation can be put to a deterministic trigger policy.
 * It will cause the lookahead for window borders to be recomputed
 * whenever a new datapoint arrives.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RecomputeLookahead {
}
