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

public class MultiExperimentDriver {

    private static final String[] SETUP_PATHS={
            "setups/test-setup-10c10t-0.txt",
            "setups/test-setup-10c10t-1.txt",
            "setups/test-setup-10c10t-2.txt",
            "setups/test-setup-10c10t-3.txt",
            "setups/test-setup-10c10t-4.txt",
            "setups/test-setup-10c10t-5.txt",
            "setups/test-setup-10c10t-6.txt",
            "setups/test-setup-10c10t-7.txt",
            "setups/test-setup-10c10t-8.txt",
            "setups/test-setup-10c10t-9.txt",
            "setups/test-setup-50c50t-0.txt",
            "setups/test-setup-50c50t-1.txt",
            "setups/test-setup-50c50t-2.txt",
            "setups/test-setup-50c50t-3.txt",
            "setups/test-setup-50c50t-4.txt",
            "setups/test-setup-50c50t-5.txt",
            "setups/test-setup-50c50t-6.txt",
            "setups/test-setup-50c50t-7.txt",
            "setups/test-setup-50c50t-8.txt",
            "setups/test-setup-50c50t-9.txt",
            "setups/test-setup-100c100t-0.txt",
            "setups/test-setup-100c100t-1.txt",
            "setups/test-setup-100c100t-2.txt",
            "setups/test-setup-100c100t-3.txt",
            "setups/test-setup-100c100t-4.txt",
            "setups/test-setup-100c100t-5.txt",
            "setups/test-setup-100c100t-6.txt",
            "setups/test-setup-100c100t-7.txt",
            "setups/test-setup-100c100t-8.txt",
            "setups/test-setup-100c100t-9.txt",
            /*"setups/test-setup-1000c1000t-0.txt",
            "setups/test-setup-1000c1000t-1.txt",
            "setups/test-setup-1000c1000t-2.txt",
            "setups/test-setup-1000c1000t-3.txt",
            "setups/test-setup-1000c1000t-4.txt",
            "setups/test-setup-1000c1000t-5.txt",
            "setups/test-setup-1000c1000t-6.txt",
            "setups/test-setup-1000c1000t-7.txt",
            "setups/test-setup-1000c1000t-8.txt",
            "setups/test-setup-1000c1000t-9.txt"*/
    };
    private static final String[] RESULT_PATHS={
            "setups/test-result-10c10t-0.txt",
            "setups/test-result-10c10t-1.txt",
            "setups/test-result-10c10t-2.txt",
            "setups/test-result-10c10t-3.txt",
            "setups/test-result-10c10t-4.txt",
            "setups/test-result-10c10t-5.txt",
            "setups/test-result-10c10t-6.txt",
            "setups/test-result-10c10t-7.txt",
            "setups/test-result-10c10t-8.txt",
            "setups/test-result-10c10t-9.txt",
            "setups/test-result-50c50t-0.txt",
            "setups/test-result-50c50t-1.txt",
            "setups/test-result-50c50t-2.txt",
            "setups/test-result-50c50t-3.txt",
            "setups/test-result-50c50t-4.txt",
            "setups/test-result-50c50t-5.txt",
            "setups/test-result-50c50t-6.txt",
            "setups/test-result-50c50t-7.txt",
            "setups/test-result-50c50t-8.txt",
            "setups/test-result-50c50t-9.txt",
            "setups/test-result-100c100t-0.txt",
            "setups/test-result-100c100t-1.txt",
            "setups/test-result-100c100t-2.txt",
            "setups/test-result-100c100t-3.txt",
            "setups/test-result-100c100t-4.txt",
            "setups/test-result-100c100t-5.txt",
            "setups/test-result-100c100t-6.txt",
            "setups/test-result-100c100t-7.txt",
            "setups/test-result-100c100t-8.txt",
            "setups/test-result-100c100t-9.txt",
            /*"setups/test-result-1000c1000t-0.txt",
            "setups/test-result-1000c1000t-1.txt",
            "setups/test-result-1000c1000t-2.txt",
            "setups/test-result-1000c1000t-3.txt",
            "setups/test-result-1000c1000t-4.txt",
            "setups/test-result-1000c1000t-5.txt",
            "setups/test-result-1000c1000t-6.txt",
            "setups/test-result-1000c1000t-7.txt",
            "setups/test-result-1000c1000t-8.txt",
            "setups/test-result-1000c1000t-9.txt"*/
    };

    public static void main(String args[]) throws Exception {

        for (int i=0;i<SETUP_PATHS.length;i++){

            System.out.println("****************START EXPERIMENT****************");
            System.out.println("INPUT: "+SETUP_PATHS[i]);
            System.out.println("OUTPUT: "+SETUP_PATHS[i]);
            System.out.println("************************************************");

            String[] expPaths={SETUP_PATHS[i],RESULT_PATHS[i]};
            ExperimentDriver.main(expPaths);

            System.out.println("****************END EXPERIMENT****************");
        }

    }

}
