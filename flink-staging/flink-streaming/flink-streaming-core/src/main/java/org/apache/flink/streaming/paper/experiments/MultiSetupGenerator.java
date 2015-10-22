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

public class MultiSetupGenerator {

    private final static int NUMBER_OF_SETUPS=10;
    private final static String SETUP_BASE_PATH="";
    private final static String SETUP_BASE_NAME="test-setup-50c50t";
    private final static String PLOTDATA_BASE_NAME="plot-range-slide-50c50t";
    private final static String SETUP_ID_SEPARATOR="-";
    private final static String FILETYPE_SUFFIX=".txt";

    public static void main (String[] args) throws Exception{
        for (int i=0;i<NUMBER_OF_SETUPS;i++){

            String[] arguments={SETUP_BASE_PATH+SETUP_BASE_NAME+SETUP_ID_SEPARATOR+i+FILETYPE_SUFFIX, SETUP_BASE_PATH+PLOTDATA_BASE_NAME+SETUP_ID_SEPARATOR+i+FILETYPE_SUFFIX};
            SetupGenerator.main(arguments);

        }
    }

}
