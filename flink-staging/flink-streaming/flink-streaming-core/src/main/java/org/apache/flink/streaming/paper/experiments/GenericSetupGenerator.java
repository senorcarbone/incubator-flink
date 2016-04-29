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

import org.aeonbits.owner.ConfigFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * This class generates a setup for our paper experiments.
 * It will create all different types of queries using normally distributed random values.
 * The class writes the setup to a file, which is later parsed by the experiment driver.
 * This allows to reply exactly the same experiment several times.
 */
public class GenericSetupGenerator {

	/**
	 * An instance of the random class used to create gaussian distributed random values
	 */
	private static final Random rnd = new Random();
	static Random rand = new Random();

	/**
	 * The main method executes the generation of the test setup.
	 *
	 * @param args not used
	 * @throws Exception Any exception which may occurs when trying to write the output file
	 */
	public static void main(String[] args) throws Exception {

		ExperimentConfig cfg = ConfigFactory.create(ExperimentConfig.class);
		String setupOutputPath;
		String plotDataOutputPath;
		if (args.length == 2) {
			setupOutputPath = args[0];
			plotDataOutputPath = args[1];
		} else {
			setupOutputPath = "test-setup.txt";
			plotDataOutputPath = "plot-range-slide1.txt";
		}

		makeSetup(setupOutputPath, plotDataOutputPath,
				cfg.regularCountMinSlide(), cfg.regularCountMaxSlide(),
				cfg.regularCountMinRange(), cfg.regularCountMaxRange(),
				cfg.numCountQueries(), cfg.numPunctQueries(), cfg.scaleFactor());

	}

	/**
	 * Creates the queries setup (all required random values) and writes them to an output file.
	 *
	 * @param regularCountMinSlide The minimal slide step for count-based queries in the regular scenarios
	 * @param regularCountMaxSlide The maximal slide step for count-based queries in the regular scenarios
	 * @param regularCountMinRange The minimal range for count-based queries in the regular scenarios
	 * @param regularCountMaxRange The maximal range for count-based queries in the regular scenarios
	 * @param numCountQueries      Number of count-based queries
	 * @throws FileNotFoundException        If the output file cannot be accessed.
	 * @throws UnsupportedEncodingException If no file using UTF8 encoding can be created.
	 */
	private static void makeSetup(String setupOutputPath, String plotDataOutputPath,
								  double regularCountMinSlide, double regularCountMaxSlide,
								  double regularCountMinRange, double regularCountMaxRange,
								  int numCountQueries, int numPunctQueries, double scaleFactor) throws FileNotFoundException, UnsupportedEncodingException {

		//make values for count based windows
		double regularCountWindowSlideDefs[] = new double[numCountQueries];
		double regularCountWindowRangeDefs[] = new double[numCountQueries];
		for (int i = 0; i < numCountQueries; i++) {
			regularCountWindowSlideDefs[i] = makeUniform(regularCountMinSlide, regularCountMaxSlide, true) * scaleFactor;
			regularCountWindowRangeDefs[i] = makeUniform(regularCountMinRange, regularCountMaxRange, true) * scaleFactor;
		}


		//Write out windows for the different scenarios (Full setup)
		PrintWriter writer = new PrintWriter(setupOutputPath, "UTF-8");
		//Write out ranges and slides for plotting them
		PrintWriter plotWriter1 = new PrintWriter(plotDataOutputPath, "UTF-8");

		//Regular scenarios
		writer.println("SCENARIO 1: REGULAR RANGE; REGULAR SLIDE");
		writer.println("\tDETERMINISTIC AND PERIODIC");
		for (int i = 0; i < numCountQueries; i++) {
			writer.println("\t\tCOUNT\t" + regularCountWindowRangeDefs[i] + "\t" + regularCountWindowSlideDefs[i] + "\t" + (10000 / regularCountWindowSlideDefs[i]));
			plotWriter1.println(regularCountWindowRangeDefs[i] + "\t" + regularCountWindowSlideDefs[i] + "\ta");
		}
		writePunctuationQueries(numPunctQueries, writer);

		writer.flush();
		writer.close();

		plotWriter1.flush();
		plotWriter1.close();
	}

	private static double makeUniform(double regularCountMinSlide, double regularCountMaxSlide, boolean b) {
		return rand.nextInt((int) ((regularCountMaxSlide - regularCountMinSlide) + 1)) + regularCountMinSlide;
	}


	private static void writePunctuationQueries(int numPunctQueries, PrintWriter writer) {
		for (int i = 0; i < numPunctQueries; i++) {
			writer.println("\t\tPUNCT\t" + i + "\t 0 \t 0");
		}
	}

}
