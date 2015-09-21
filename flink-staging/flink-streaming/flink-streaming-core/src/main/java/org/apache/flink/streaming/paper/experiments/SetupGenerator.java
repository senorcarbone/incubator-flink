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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Random;

/**
 * This class generates a setup for our paper experiments.
 * It will create all different types of queries using normally distributed random values.
 * The class writes the setup to a file, which is later parsed by the experiment driver.
 * This allows to reply exactly the same experiment several times.
 */
public class SetupGenerator {

    /**
     * An instance of the random class used to create gaussian distributed random values
     */
    private static final Random rnd = new Random();

    /**
     * The main method executes the generation of the test setup.
     *
     * @param args not used
     * @throws Exception Any exception which may occurs when trying to write the output file
     */
    public static void main(String[] args) throws Exception {

        ExperimentConfig cfg = ConfigFactory.create(ExperimentConfig.class);

        makeSetup(cfg.countExperimentSize(), cfg.timeExperimentSize(),
                cfg.regularCountMinSlide() * cfg.scaleFactor(), cfg.regularCountMaxSlide() * cfg.scaleFactor(),
                cfg.lowerCountMinSlide() * cfg.scaleFactor(), cfg.lowerCountMaxSlide() * cfg.scaleFactor(),
                cfg.upperCountMinSlide() * cfg.scaleFactor(), cfg.upperCountMaxSlide() * cfg.scaleFactor(),
                cfg.regularCountMinRange() * cfg.scaleFactor(), cfg.regularCountMaxRange() * cfg.scaleFactor(),
                cfg.lowerCountMinRange() * cfg.scaleFactor(), cfg.lowerCountMaxRange() * cfg.scaleFactor(),
                cfg.upperCountMinRange() * cfg.scaleFactor(), cfg.upperCountMaxRange() * cfg.scaleFactor(),
                cfg.regularTimeMinSlide() * cfg.scaleFactor(), cfg.regularTimeMaxSlide() * cfg.scaleFactor(),
                cfg.lowerTimeMinSlide() * cfg.scaleFactor(), cfg.lowerTimeMaxSlide() * cfg.scaleFactor(),
                cfg.upperTimeMinSlide() * cfg.scaleFactor(), cfg.upperTimeMaxSlide() * cfg.scaleFactor(),
                cfg.regularTimeMinRange() * cfg.scaleFactor(), cfg.regularTimeMaxRange() * cfg.scaleFactor(),
                cfg.lowerTimeMinRange() * cfg.scaleFactor(), cfg.lowerTimeMaxRange() * cfg.scaleFactor(),
                cfg.upperTimeMinRange() * cfg.scaleFactor(), cfg.upperTimeMaxRange() * cfg.scaleFactor(),
                cfg.numTimeQueries(), cfg.numCountQueries(), cfg.scenarioRegularSlideRegularRange(),
                cfg.scenarioSmallSlideRegularRange(), cfg.scenarioHighSlideRegularRange(),
                cfg.scenarioRegularSlideSmallRange(), cfg.scenarioRegularSlideHighRange(),
                cfg.scenarioSmallSlideHighRange(), cfg.scenarioHighSlideHighRange(),
                cfg.scenarioSmallSlideSmallRange(), cfg.scenarioHighSlideSmallRange(), cfg.generateRandomWalks());

    }

    /**
     * Creates the queries setup (all required random values) and writes them to an output file.
     *
     * @param regularCountMinSlide The minimal slide step for count-based queries in the regular scenario
     * @param regularCountMaxSlide The maximal slide step for count-based queries in the regular scenario
     * @param lowerCountMinSlide   The minimal slide step for count-based queries in the low-slide scenario
     * @param lowerCountMaxSlide   The maximal slide step for count-based queries in the low-slide scenario
     * @param upperCountMinSlide   The minimal slide step for count-based queries in the high-slide scenario
     * @param upperCountMaxSlide   The maximal slide step for count-based queries in the high-slide scenario
     * @param regularCountMinRange The minimal range for count-based queries in the regular scenario
     * @param regularCountMaxRange The maximal range for count-based queries in the regular scenario
     * @param lowerCountMinRange   The minimal range for count-based queries in the low-range scenario
     * @param lowerCountMaxRange   The maximal range for count-based queries in the low-range scenario
     * @param upperCountMinRange   The minimal range for count-based queries in the high-range scenario
     * @param upperCountMaxRange   The maximal range for count-based queries in the high-range scenario
     * @param regularTimeMinSlide  The minimal slide step for time-based queries in the regular scenario
     * @param regularTimeMaxSlide  The maximal slide step for time-based queries in the regular scenario
     * @param lowerTimeMinSlide    The minimal slide step for time-based queries in the low-slide scenario
     * @param lowerTimeMaxSlide    The maximal slide step for time-based queries in the low-slide scenario
     * @param upperTimeMinSlide    The minimal slide step for time-based queries in the high-slide scenario
     * @param upperTimeMaxSlide    The maximal slide step for time-based queries in the high-slide scenario
     * @param regularTimeMinRange  The minimal range for time-based queries in the regular scenario
     * @param regularTimeMaxRange  The maximal range for time-based queries in the regular scenario
     * @param lowerTimeMinRange    The minimal range for time-based queries in the low-range scenario
     * @param lowerTimeMaxRange    The maximal range for time-based queries in the low-range scenario
     * @param upperTimeMinRange    The minimal range for time-based queries in the high-range scenario
     * @param upperTimeMaxRange    The maximal range for time-based queries in the high-range scenario
     * @param numTimeQueries       Number of time-based queries
     * @param numCountQueries      Number of count-based queries
     * @throws FileNotFoundException        If the output file cannot be accessed.
     * @throws UnsupportedEncodingException If no file using UTF8 encoding can be created.
     */
    private static void makeSetup(int countExperimentSize, int timeExperimentSize,
                                  double regularCountMinSlide, double regularCountMaxSlide, double lowerCountMinSlide, double lowerCountMaxSlide, double upperCountMinSlide, double upperCountMaxSlide,
                                  double regularCountMinRange, double regularCountMaxRange, double lowerCountMinRange, double lowerCountMaxRange, double upperCountMinRange, double upperCountMaxRange,
                                  double regularTimeMinSlide, double regularTimeMaxSlide, double lowerTimeMinSlide, double lowerTimeMaxSlide, double upperTimeMinSlide, double upperTimeMaxSlide,
                                  double regularTimeMinRange, double regularTimeMaxRange, double lowerTimeMinRange, double lowerTimeMaxRange, double upperTimeMinRange, double upperTimeMaxRange,
                                  int numTimeQueries, int numCountQueries, boolean scenarioRegularSlideRegularRange, boolean scenarioSmallSlideRegularRange, boolean scenarioHighSlideRegularRange,
                                  boolean scenarioRegularSlideSmallRange, boolean scenarioRegularSlideHighRange, boolean scenarioSmallSlideHighRange, boolean scenarioHighSlideHighRange,
                                  boolean scenarioSmallSlideSmallRange, boolean scenarioHighSlideSmallRange, boolean generateRandomWalk) throws FileNotFoundException, UnsupportedEncodingException {

        //make values for count based windows
        double regularCountWindowSlideDefs[] = new double[numCountQueries];
        double regularCountWindowRangeDefs[] = new double[numCountQueries];
        double lowerCountWindowSlideDefs[] = new double[numCountQueries];
        double lowerCountWindowRangeDefs[] = new double[numCountQueries];
        double upperCountWindowSlideDefs[] = new double[numCountQueries];
        double upperCountWindowRangeDefs[] = new double[numCountQueries];
        for (int i = 0; i < numCountQueries; i++) {
            regularCountWindowSlideDefs[i] = makeGaussian(regularCountMinSlide, regularCountMaxSlide, true);
            regularCountWindowRangeDefs[i] = makeGaussian(regularCountMinRange, regularCountMaxRange, true);
            lowerCountWindowSlideDefs[i] = makeGaussian(lowerCountMinSlide, lowerCountMaxSlide, true);
            lowerCountWindowRangeDefs[i] = makeGaussian(lowerCountMinRange, lowerCountMaxRange, true);
            upperCountWindowSlideDefs[i] = makeGaussian(upperCountMinSlide, upperCountMaxSlide, true);
            upperCountWindowRangeDefs[i] = makeGaussian(upperCountMinRange, upperCountMaxRange, true);
        }

        //make values for time based windows
        double regularTimeWindowSlideDefs[] = new double[numTimeQueries];
        double regularTimeWindowRangeDefs[] = new double[numTimeQueries];
        double lowerTimeWindowSlideDefs[] = new double[numTimeQueries];
        double lowerTimeWindowRangeDefs[] = new double[numTimeQueries];
        double upperTimeWindowSlideDefs[] = new double[numTimeQueries];
        double upperTimeWindowRangeDefs[] = new double[numTimeQueries];
        for (int i = 0; i < numTimeQueries; i++) {
            regularTimeWindowSlideDefs[i] = makeGaussian(regularTimeMinSlide, regularTimeMaxSlide, false);
            regularTimeWindowRangeDefs[i] = makeGaussian(regularTimeMinRange, regularTimeMaxRange, false);
            lowerTimeWindowSlideDefs[i] = makeGaussian(lowerTimeMinSlide, lowerTimeMaxSlide, false);
            lowerTimeWindowRangeDefs[i] = makeGaussian(lowerTimeMinRange, lowerTimeMaxRange, false);
            upperTimeWindowSlideDefs[i] = makeGaussian(upperTimeMinSlide, upperTimeMaxSlide, false);
            upperTimeWindowRangeDefs[i] = makeGaussian(upperTimeMinRange, upperTimeMaxRange, false);
        }

        //Write out windows for the different scenarios (Full setup)
        PrintWriter writer = new PrintWriter("test-setup.txt", "UTF-8");
        //Write out ranges and slides for plotting them
        PrintWriter plotWriter1 = new PrintWriter("plot-range-slide1.txt", "UTF-8");

        //Regular scenario
        if (scenarioRegularSlideRegularRange){
            writer.println("SCENARIO 1: REGULAR RANGE; REGULAR SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + regularCountWindowRangeDefs[i] + "\t" + regularCountWindowSlideDefs[i] + "\t" + (10000 / regularCountWindowSlideDefs[i]));
                plotWriter1.println(regularCountWindowRangeDefs[i] + "\t" + regularCountWindowSlideDefs[i] + "\ta");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + regularTimeWindowRangeDefs[i] + "\t" + regularTimeWindowSlideDefs[i] + "\t" + (10000 / regularTimeWindowSlideDefs[i]));
            }
        }

        //Slide Regular, Range low
        if (scenarioRegularSlideSmallRange){
            writer.println("SCENARIO 2: LOW RANGE; REGULAR SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + lowerCountWindowRangeDefs[i] + "\t" + regularCountWindowSlideDefs[i] + "\t" + (10000 / regularCountWindowSlideDefs[i]));
                plotWriter1.println(lowerCountWindowRangeDefs[i] + "\t" + regularCountWindowSlideDefs[i] + "\tb");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + lowerTimeWindowRangeDefs[i] + "\t" + regularTimeWindowSlideDefs[i] + "\t" + (10000 / regularTimeWindowSlideDefs[i]));
            }
        }

        //Slide Regular, High range
        if (scenarioRegularSlideHighRange){
            writer.println("SCENARIO 3: HIGH RANGE; REGULAR SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + upperCountWindowRangeDefs[i] + "\t" + regularCountWindowSlideDefs[i] + "\t" + (10000 / regularCountWindowSlideDefs[i]));
                plotWriter1.println(upperCountWindowRangeDefs[i] + "\t" + regularCountWindowSlideDefs[i] + "\tc");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + upperTimeWindowRangeDefs[i] + "\t" + regularTimeWindowSlideDefs[i] + "\t" + (10000 / regularTimeWindowSlideDefs[i]));
            }
        }

        //Slide low, Range regular
        if (scenarioSmallSlideRegularRange){
            writer.println("SCENARIO 4: REGULAR RANGE; LOW SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + regularCountWindowRangeDefs[i] + "\t" + lowerCountWindowSlideDefs[i] + "\t" + (10000 / regularCountWindowSlideDefs[i]));
                plotWriter1.println(regularCountWindowRangeDefs[i] + "\t" + lowerCountWindowSlideDefs[i] + "\td");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + regularTimeWindowRangeDefs[i] + "\t" + lowerTimeWindowSlideDefs[i] + "\t" + (10000 / regularTimeWindowSlideDefs[i]));
            }
        }

        //Slide high, Range regular
        if (scenarioHighSlideRegularRange){
            writer.println("SCENARIO 5: REGULAR RANGE; HIGH SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + regularCountWindowRangeDefs[i] + "\t" + upperCountWindowSlideDefs[i] + "\t" + (10000 / regularCountWindowSlideDefs[i]));
                plotWriter1.println(regularCountWindowRangeDefs[i] + "\t" + upperCountWindowSlideDefs[i] + "\te");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + regularTimeWindowRangeDefs[i] + "\t" + upperTimeWindowSlideDefs[i] + "\t" + (10000 / regularTimeWindowSlideDefs[i]));
            }
        }

        if (scenarioHighSlideHighRange){
            writer.println("SCENARIO 6: HIGH RANGE; HIGH SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + upperCountWindowRangeDefs[i] + "\t" + upperCountWindowSlideDefs[i] + "\t" + (10000 / upperCountWindowSlideDefs[i]));
                plotWriter1.println(upperCountWindowRangeDefs[i] + "\t" + upperCountWindowSlideDefs[i] + "\tf");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + upperTimeWindowRangeDefs[i] + "\t" + upperTimeWindowSlideDefs[i] + "\t" + (10000 / upperTimeWindowSlideDefs[i]));
            }
        }

        if (scenarioHighSlideSmallRange){
            writer.println("SCENARIO 7: LOW RANGE; HIGH SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + lowerCountWindowRangeDefs[i] + "\t" + upperCountWindowSlideDefs[i] + "\t" + (10000 / upperCountWindowSlideDefs[i]));
                plotWriter1.println(lowerCountWindowRangeDefs[i] + "\t" + upperCountWindowSlideDefs[i] + "\tg");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + lowerTimeWindowRangeDefs[i] + "\t" + upperTimeWindowSlideDefs[i] + "\t" + (10000 / upperTimeWindowSlideDefs[i]));
            }
        }

        if (scenarioSmallSlideSmallRange){
            writer.println("SCENARIO 8: LOW RANGE; LOW SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + lowerCountWindowRangeDefs[i] + "\t" + lowerCountWindowSlideDefs[i] + "\t" + (10000 / lowerCountWindowSlideDefs[i]));
                plotWriter1.println(lowerCountWindowRangeDefs[i] + "\t" + lowerCountWindowSlideDefs[i] + "\th");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + lowerTimeWindowRangeDefs[i] + "\t" + lowerTimeWindowSlideDefs[i] + "\t" + (10000 / lowerTimeWindowSlideDefs[i]));
            }
        }

        if (scenarioSmallSlideHighRange){
            writer.println("SCENARIO 9: HIGH RANGE; LOW SLIDE");
            writer.println("\tDETERMINISTIC AND PERIODIC");
            for (int i = 0; i < numCountQueries; i++) {
                writer.println("\t\tCOUNT\t" + upperCountWindowRangeDefs[i] + "\t" + lowerCountWindowSlideDefs[i] + "\t" + (10000 / lowerCountWindowSlideDefs[i]));
                plotWriter1.println(upperCountWindowRangeDefs[i] + "\t" + lowerCountWindowSlideDefs[i] + "\th");
            }
            for (int i = 0; i < numTimeQueries; i++) {
                writer.println("\t\tTIME\t" + upperTimeWindowRangeDefs[i] + "\t" + lowerTimeWindowSlideDefs[i] + "\t" + (10000 / lowerTimeWindowSlideDefs[i]));
            }
        }


        //Generate random walk window ends if requested
        if (generateRandomWalk){
            //Random Walk cases:
            //Generate randomly places window ends per query:
            //Regular scenario
            if (scenarioRegularSlideRegularRange){
                writer.println("SCENARIO 1: REGULAR RANGE; REGULAR SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + regularCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, regularCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + regularTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, regularTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

            if (scenarioRegularSlideSmallRange){
                writer.println("SCENARIO 2: LOW RANGE; REGULAR SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                //Slide Regular, Range low
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + lowerCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, regularCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + lowerTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, regularTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

            if (scenarioRegularSlideHighRange){
                writer.println("SCENARIO 3: HIGH RANGE; REGULAR SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                //Slide Regular, High range
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + upperCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, regularCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + upperTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, regularTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

            if (scenarioSmallSlideRegularRange){
                writer.println("SCENARIO 4: REGULAR RANGE; LOW SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                //Slide low, Range regular
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + regularCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, lowerCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + regularTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, lowerTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

            if (scenarioHighSlideRegularRange){
                writer.println("SCENARIO 5: REGULAR RANGE; HIGH SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + regularCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, upperCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + regularTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, upperTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

            if (scenarioHighSlideHighRange){
                writer.println("SCENARIO 6: HIGH RANGE; HIGH SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + upperCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, upperCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + upperTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, upperTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

            if (scenarioHighSlideSmallRange){
                writer.println("SCENARIO 7: LOW RANGE; HIGH SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + lowerCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, upperCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + lowerTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, upperTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

            if (scenarioSmallSlideSmallRange){
                writer.println("SCENARIO 8: LOW RANGE; LOW SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + lowerCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, lowerCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + lowerTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, lowerTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

            if (scenarioSmallSlideHighRange){
                writer.println("SCENARIO 9: HIGH RANGE; LOW SLIDE");
                writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
                for (int i = 0; i < numCountQueries; i++) {
                    writer.print("\t\tCOUNT\t" + upperCountWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(countExperimentSize, lowerCountWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
                for (int i = 0; i < numTimeQueries; i++) {
                    writer.print("\t\tCOUNT\t" + upperTimeWindowRangeDefs[i] + "\t");
                    for (Double d : makeRandomValueList(timeExperimentSize, lowerTimeWindowSlideDefs[i])) {
                        writer.print(d + " ");
                    }
                    writer.println();
                }
            }

        }

        writer.flush();
        writer.close();

        plotWriter1.flush();
        plotWriter1.close();
    }

    /**
     * Creates an ordered list of random values reaching from 0 to maxValue.
     * Exactly maxValue/slideSize random values are generated.
     *
     * @param maxValue  the maximal possible return window
     * @param slideSize the slide size of the window (used to calculate the required number of random values)
     * @return A ordered list of random values reaching from 0 to maxValue.
     */
    private static Double[] makeRandomValueList(int maxValue, double slideSize) {
        LinkedList<Double> values = new LinkedList<Double>();

        /*
        The random walk window ends are calculated as follows:
        1) maxValue is set to the maximum number of tuples which will be processed in the experiment.
        2) maxValue is divided by the slideSize of the corresponding periodic query.
        3) Now we have the number of window emissions of the periodic case. We want to have the same in the random walk.
        4) We generate maxValue/slideSize random values in the range from 0 to maxValue
        5) We order the random value list. => The resulting ordered list gives the window ends for the random walk.
         */
        for (int i = 0; i <= maxValue / slideSize; i++) {
            double value = 0;
            while (value < 1) {
                value = rnd.nextDouble() * maxValue;
            }
            values.add(value);
        }

        Collections.sort(values, new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                if (o1 < o2) return -1;
                else if (o1.equals(o2)) return 0;
                else return 1;
            }
        });

        return values.toArray(new Double[1]);
    }

    /**
     * Creates a random value between min and max.
     * The random values are gaussian distributed between min and max
     *
     * @param min the minimal possible return value
     * @param max the maximal possible return value
     * @return a random value between min and max (gaussion distributed)
     */
    private static double makeGaussian(double min, double max, boolean noDecimal) {
        double value;
        double sdv = (max - min) / 7;
        double mean = min + ((max - min) / 2);
        while (true) {
            if ((value = rnd.nextGaussian() * sdv + mean) >= min && value <= max) {
                return !noDecimal ? value : (int) value;
            }
        }
    }

}
