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

package org.apache.flink.streaming.api.windowing.policy;

import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

public class DeterministicTimeEvictionPolicy<DATA> extends TimeEvictionPolicy<DATA> implements DeterministicEvictionPolicy<DATA> {

    long granularity;

    public DeterministicTimeEvictionPolicy(long granularity, TimestampWrapper<DATA> timestampWrapper){
        super(granularity,timestampWrapper);
        this.granularity=granularity;
    }

    @Override
    public double getLowerBorder(double upperBorder) {
        if (upperBorder-granularity<0){
            return 0;
        } else {
            return upperBorder-(double)granularity;
        }
    }
}
