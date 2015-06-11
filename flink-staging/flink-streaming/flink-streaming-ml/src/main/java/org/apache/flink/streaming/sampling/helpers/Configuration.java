package org.apache.flink.streaming.sampling.helpers;/*
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

/**
 * Created by marthavk on 2015-06-04.
 */
public class Configuration {

	/**
	 * GENERAL PROPERTIES *
	 */
	public static long maxCount = 500000;

	//Window sizes (count-based and time-based)
	public static int countWindowSize = 50000;
	public static long timeWindowSize = 100;

	//Buffer Sizes
	public static int SAMPLE_SIZE_1000 = 1000;
	public static int SAMPLE_SIZE_5000 = 5000;
	public static int SAMPLE_SIZE_10000 = 10000;
	public static int SAMPLE_SIZE_50000 = 50000;

	//Output Rate
	public static double outputRate = 0.5;

	/**
	 * GAUSSIAN DISTRIBUTION PROPERTIES *
	 */
	//Initial and target mean and standard deviation
	public static double meanInit = 0;
	public static double stDevInit = 1;
	public static double meanTarget = 2;
	public static double stDevTarget = 1;

	//Defines smoothness of distribution
	public static boolean isSmooth = false;

	//Maximum number of generated numbers
	public static int numberOfSteps = 2;
	public static int stablePoints = 0;

	//Outlier Rate (equals the probability of generating an outlier)
	public static double outlierRate = 0.1;

	/**
	 * OTHER PROPERTIES *
	 */
	//Page Hinkley Test parameters
	public static double lambda = 15.0;
	public static double delta = 3.0;

	/**
	 * PATHS
	 */
	public static String outputPath;
}
