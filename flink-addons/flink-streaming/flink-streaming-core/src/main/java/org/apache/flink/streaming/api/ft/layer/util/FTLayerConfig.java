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

package org.apache.flink.streaming.api.ft.layer.util;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;

public class FTLayerConfig {

	private static final String BUFFER_TIMEOUT = "buffer timeout";
	private static final String NUMBER_OF_OUTPUTS = "number of outputs";
	private static final String SOURCE_SUCCESSIVES = "source successives";
	private static final String NUMBER_OF_SOURCES = "number of sources";

	private Configuration config;

	// TODO check whether values are set & throw exceptions

	public FTLayerConfig(Configuration config) {
		this.config = config;
	}

	public int getNumberOfSources() {
		return config.getInteger(NUMBER_OF_SOURCES, 0);
	}

	public void setNumberOfSources(int numberOfSources) {
		config.setInteger(NUMBER_OF_SOURCES, numberOfSources);
	}

	public int[][] getSourceSuccesives() {
		try {
			return (int[][]) InstantiationUtil.deserializeObject(config.getBytes(
					SOURCE_SUCCESSIVES, new byte[0]), Thread.currentThread()
					.getContextClassLoader());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void setSourceSuccesives(int[][] sourceSuccesives) {
		try {
			InstantiationUtil.writeObjectToConfig(sourceSuccesives, config, SOURCE_SUCCESSIVES);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int getNumberOfOutputs() {
		return config.getInteger(NUMBER_OF_OUTPUTS, -1);
	}

	public void setNumberOfOutputs(int numberOfOutputs) {
		config.setInteger(NUMBER_OF_OUTPUTS, numberOfOutputs);
	}
	
	public long getBufferTimeout() {
		return config.getLong(BUFFER_TIMEOUT, 0L);
	}
	
	public void setBufferTimeout(long timeout) {
		config.setLong(BUFFER_TIMEOUT, timeout);
	}

}
