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

package org.apache.flink.streaming.paper.adwinPolicies.helper;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ADWINSampleSource implements SourceFunction<Integer>{

    boolean isRunning=true;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        for (int p=0;p<1000;p++){
            ctx.collect(f(p));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private static int f(int p) {
        //Abrupt Change
        return (p<500 ? 1000:500);
    }

}
