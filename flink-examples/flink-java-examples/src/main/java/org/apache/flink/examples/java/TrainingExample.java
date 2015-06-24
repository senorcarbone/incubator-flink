package org.apache.flink.examples.java;/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by marthavk on 2015-06-17.
 */
public class TrainingExample {
	public static void main(String[] args) throws Exception {
		// get an ExecutionEnvironment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read all fields
		DataSet<Tuple6<String, String, String, String, String, String>> mails =
				env.readCsvFile("/home/marthavk/Desktop/trainingFLink/flinkMails.del")
						.lineDelimiter("##//##")
						.fieldDelimiter("#|#")
						.types(String.class, String.class, String.class,
								String.class, String.class, String.class);
		mails.print();

/*
		// read sender and body fields
		DataSet<Tuple2<String, String>> senderBody =
				env.readCsvFile("/your/output/mail-data")
						.lineDelimiter("##//##")
						.fieldDelimiter("#|#")
						.includeFields("00101")
						.types(String.class, String.class);
*/

	}
}
