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

package org.apache.flink.streaming.paper.experiments;

import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTriggerPolicy;

/**
 * This policy receives a list of double values and triggers whenever the next value in the list was exceeded.
 * This can be used to execute a random walk with pre-generated random values.
 * (Deterministic version of @link{RandomWalkTriggerPolicy})
 * @param <DATA> the type of input tuples handled by this trigger.
 */
public class DeterministicRandomWalkTriggerPolicy<DATA> extends RandomWalkTriggerPolicy<DATA> implements DeterministicTriggerPolicy<DATA>{

    public DeterministicRandomWalkTriggerPolicy(Double[] windowEnds, Extractor<DATA,Double> extractor){
        super(windowEnds,extractor);
    }

    @Override
    public double getNextTriggerPosition(double previouseTriggerPosition) {
        i++;
        return windowEnds[i-1];
    }
}
