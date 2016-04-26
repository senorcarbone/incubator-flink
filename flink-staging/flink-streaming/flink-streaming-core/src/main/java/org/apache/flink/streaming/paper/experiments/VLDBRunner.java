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

public class VLDBRunner {

	/**
	 * Reminder for datasets:
	 * 
	 * "/Users/carbone/workspace/datasets/DEBS2012-ChallengeData.txt"
	 * "/Users/carbone/workspace/datasets/debs-sample.csv"
	 */
	
    private static final String[] SETUP_PATHS={
//			"setups/testtime.txt"
//			"setups/test-setup-10p-0.txt",
            "setups/test-setup-1c0t-0.txt"
    };
    private static final String[] RESULT_PATHS={
//			"setups/testtime-results.txt"
//			"setups/test-result-10p-0.txt",
			"setups/test-result-1c0t-0.txt"
    };

    public static void main(String args[]) throws Exception {

        for (int i=0;i<SETUP_PATHS.length;i++){

            System.out.println("****************START EXPERIMENT****************");
            System.out.println("INPUT: "+SETUP_PATHS[i]);
            System.out.println("OUTPUT: "+SETUP_PATHS[i]);
            System.out.println("************************************************");

            String[] expPaths={args[0],SETUP_PATHS[i],RESULT_PATHS[i]};
            DEBSExpDriver.main(expPaths);

            System.out.println("****************END EXPERIMENT****************");
        }
    }

}
