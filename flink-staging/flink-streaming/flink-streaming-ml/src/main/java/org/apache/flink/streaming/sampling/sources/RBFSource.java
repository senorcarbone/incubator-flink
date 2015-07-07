/*
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
package org.apache.flink.streaming.sampling.sources;
import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.sampling.helpers.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


/**
 * Created by marthavk on 2015-06-24.
 */
public class RBFSource implements SourceFunction<String> {

	boolean running=true;
	String filePath;
	//long timeInSeconds = 300;

	public RBFSource(String fp) {
		filePath = fp;
	}


	@Override
	public void run(final SourceContext<String> ctx) throws Exception {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		//TextInputFormat format = new TextInputFormat(new Path(filePath));
		//TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
		//logic for the thread

		//long totalTimeInNanos = (long) (timeInSeconds*10*Math.pow(10,9));
		//System.out.println(totalTimeInNanos);
		//long sleepTimeInNanos = timeInSeconds/linesOfFile;
		long rate = (long) (1000/(Configuration.millis+Math.pow(10,-9)*Configuration.nanos));

		System.out.println("source rate: " + rate + " p.s.");
		File file = new File(filePath);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (running) {

			try {
				Thread.sleep(Configuration.millis, Configuration.nanos);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				String text = reader != null ? reader.readLine() : null;
				if (text != null) {
					ctx.collect(text);
				} else {
					running = false;
				}

			} catch (IndexOutOfBoundsException ignored) {
			} catch (IllegalArgumentException ignored) {
			} catch (NullPointerException ignored) {
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

	@Override
	public void cancel() {

	}
}
