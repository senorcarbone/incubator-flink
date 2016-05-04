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
//			"setups/exp2-1p.txt",
//			"setups/exp2-10p.txt",4
//			"setups/exp2-20p.txt"
			"setups/exp2-1p.txt",
			"setups/exp2-10p.txt",
			"setups/exp2-20p.txt",
            "setups/exp2-30p.txt",
            "setups/exp2-40p.txt",
            "setups/exp2-50p.txt",
            "setups/exp2-60p.txt",
            "setups/exp2-70p.txt",
            "setups/exp2-80p.txt",
            "setups/exp2-90p.txt",
            "setups/exp2-100p.txt"
//			"setups/exp1-40c.txt"
    };
    private static final String[] RESULT_PATHS={
//			"setups/exp2-1p-results.txt",
//			"setups/exp2-10p-results.txt",
//			"setups/exp2-20p-results.txt"
//			"setups/exp1-1c-results.txt",
//			"setups/exp1-10c-results.txt",
            "setups/exp2-1pres.txt",
            "setups/exp2-10pres.txt",
            "setups/exp2-20pres.txt",
            "setups/exp2-30pres.txt",
            "setups/exp2-40pres.txt",
            "setups/exp2-50pres.txt",
            "setups/exp2-60pres.txt",
            "setups/exp2-70pres.txt",
            "setups/exp2-80pres.txt",
            "setups/exp2-90pres.txt",
            "setups/exp2-100pres.txt"
//			"setups/exp1-40c-results.txt"
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
