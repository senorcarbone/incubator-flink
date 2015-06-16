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

/**
 * A deterministic eviction policy allows to apply border to border pre-aggregation means.
 * Eviction policies are deterministic if they can calculate the begin (lower border) of a window,
 * for any given window end (upper Border)
 */
public interface DeterministicEvictionPolicy<DATA> extends EvictionPolicy<DATA> {

    /**
     * This method receives the upper border (window end) as parameter.
     * It returns the lower border (window begin) belonging to the given upper border.
     * @param upperBorder The upper border (window end)
     * @return The lower boder (window begin)
     */
    public long getLowerBorder(long upperBorder);

}
