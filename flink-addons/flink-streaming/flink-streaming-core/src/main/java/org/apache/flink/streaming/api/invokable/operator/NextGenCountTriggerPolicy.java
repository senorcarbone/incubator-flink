/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.invokable.operator;

/**
 * This policy triggers at every n'th element.
 * @param <IN> The type of the data points which is handled by this policy
 */
public class NextGenCountTriggerPolicy<IN> implements NextGenTriggerPolicy<IN> {

    /**
     * Auto generated version ID
     */
    private static final long serialVersionUID = -6357200688886103968L;

    private static final int DEFAULT_START_VALUE = 0;

    private int counter;
    private int max;

    /**
     * This constructor will set up a count based trigger, which triggers
     * after max elements have arrived.
     * @param max The number of arriving elements before the trigger occurs.
     */
    public NextGenCountTriggerPolicy(int max) {
        this(max, DEFAULT_START_VALUE);
    }

    /**
     * In addition to {@link NextGenCountTriggerPolicy#NextGenCountTriggerPolicy(int)}
     * this constructor allows to set a custom start value for the element counter.
     * @param max The number of arriving elements before the trigger occurs.
     * @param startValue The start value for the counter of arriving elements.
     * @see NextGenCountTriggerPolicy#NextGenCountTriggerPolicy(int)
     */
    public NextGenCountTriggerPolicy(int max, int startValue) {
        this.max = max;
        this.counter = startValue;
    }

    @Override
    public boolean notifyTrigger(IN datapoint) {
        if (counter == max) {
            //The current data point will be part of the next window!
            //Therefore the counter needs to be set to one already.
            counter = 1;
            return true;
        } else {
            counter++;
            return false;
        }
    }


}
