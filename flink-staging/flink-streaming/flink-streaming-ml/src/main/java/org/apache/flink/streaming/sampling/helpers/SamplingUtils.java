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
package org.apache.flink.streaming.sampling.helpers;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.samplers.Buffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

/**
 * Created by marthavk on 2015-03-31.
 */

public final class SamplingUtils {
	public static final int REAL_DISTRIBUTION = 1;
	public static final int EMPIRICAL_DISTRIBUTION = 2;

	public static String path = "/home/marthavk/workspace/flink/flink-staging/flink-streaming" +
			"/flink-streaming-ml/src/main/resources/";
	public static String covertypePath = "/home/marthavk/Desktop/thesis-all-docs/resources/dataSets/forestCovertype/covtype.libsvm.binary";
	public static String randomRBFPath = "/home/marthavk/Desktop/thesis-all-docs/resources/dataSets/randomRBF/randomRBF-10M.arff";
	public static String externalPath = "/home/marthavk/Desktop/thesis-all-docs/resources/";
	static Random rand = new Random();

	public static boolean flip(int sides) {
		return (rand.nextDouble() * sides < 1);
	}


	public static boolean flip(double probability) {
		return (rand.nextDouble() < probability);
	}

	/**
	 * @param min
	 * @param max
	 * @return a random int between min and max (both inclusive)
	 */
	public static int randomBoundedInteger(int min, int max) {
		int length = max - min + 1;
		return (rand.nextInt(length) + min);
	}

	public static long randomBoundedLong(Long min, Long max) {
		int length = (int) (max - min + 1);
		int offset = rand.nextInt(length);
		return min + offset;
	}

	public static int nextRandInt(int n) {
		return rand.nextInt(n);
	}

	/**
	 * Reads properties file
	 *
	 * @param filename the path of the file
	 * @return the file as Properties Object
	 */
	public static Properties readProperties(String filename) {
		Properties properties = new Properties();
		File file = new File(filename);
		try {
			FileInputStream fileInput = new FileInputStream(file);
			properties.load(fileInput);
			fileInput.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}

	public static int min(int o1, int o2) {
		return o1 < o2 ? o1 : o2;
	}

	public static long min(long l1, long l2) {
		return l1 < l2 ? l1 : l2;
	}

	public static long max(long l1, long l2) {
		return l1 > l2 ? l1 : l2;
	}

	public static double randomPriority() {
		return rand.nextDouble();
	}

	public static SummaryStatistics getStats(Buffer<Double> sample) {
		SummaryStatistics stats = new SummaryStatistics();
		for (Double value : sample.getSample()) {
			stats.addValue(value);
		}
		return stats;
	}

	public static SummaryStatistics getStats(ArrayList<Double> sample) {
		SummaryStatistics stats = new SummaryStatistics();
		for (Double value : sample) {
			stats.addValue(value);
		}
		return stats;
	}

	public static double bhattacharyyaDistance(GaussianDistribution greal, GaussianDistribution gsampled) {

		//Bhattacharyya distance
		double m1 = greal.getMean();
		double m2 = gsampled.getMean();
		double s1 = greal.getStandardDeviation();
		double s2 = gsampled.getStandardDeviation();

		double factor1 = Math.pow(s1, 2) / Math.pow(s2, 2) + Math.pow(s2, 2) / Math.pow(s1, 2) + 2;
		double factor2 = Math.pow((m1 - m2), 2) / (Math.pow(s1, 2) + Math.pow(s2, 2));
		double distance = (0.25) * Math.log((0.25) * factor1) + (0.25) * factor2;
		return distance;

	}

	public static double squaredDistance(double x1, double x2) {
		return Math.pow((x1 - x2), 2);
	}


}
