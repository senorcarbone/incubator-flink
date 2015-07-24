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

package org.apache.flink.streaming.paper.adwinPolicies;

import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * This policy triggers whenever ADWIN decides to split the window.
 * If this is the case, a concept drift was detected.
 */
public class ADWINTriggerPolicy implements TriggerPolicy<Integer> {

    private boolean delayed;
    private boolean triggered=false;
    private ADWINCombinedPolicy adwin = new ADWINCombinedPolicy();

    /**
     * Creates an instance of the ADWINCombinedPolicy-Based trigger policy for concept drift detection.
     *
     * If the default constructor is used, a concept drift will trigger one data-item arrival after it was detected.
     * Due to the action order in the window operator, this allows an eviction policy to adjust the buffer before
     * a result gets emitted due to the occurred trigger.
     */
    private ADWINTriggerPolicy(){
        this(true);
    }

    /**
     * Creates an instance of the ADWINCombinedPolicy-Based trigger policy for concept drift detection.
     *
     * @param delayed if this is set to true a concept drift will trigger one data-item arrival after it was detected.
     * Due to the action order in the window operator, this allows an eviction policy to adjust the buffer before
     * a result gets emitted due to the occurred trigger. If this is set to false, a detected concept drift will trigger
     * immediately.
     */
    private ADWINTriggerPolicy(boolean delayed){
        this.delayed=delayed;
    }

    @Override
    public boolean notifyTrigger(Integer datapoint) {
        if (delayed){
            boolean tmp=this.triggered;
            this.triggered= adwin.notifyTrigger(datapoint);
            return tmp;
        } else {
            return adwin.notifyTrigger(datapoint);
        }

    }

}
