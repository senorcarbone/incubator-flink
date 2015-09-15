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
     * The number of time based queries to be generated
     */
    private static final int numTimeQueries=100;

    /**
     * The number of count based queries to be generated
     */
    private static final int numCountQueries=100;

    /**
     * The number of tuples which will be processed in the experiment.
     * Make sure to select a value which is higher than the actual number of tuples covered in the experiment to
     * allow deterministic not periodic policies to compute their look-ahead.
     */
    private static final int countExperimentSize=2000000;

    /**
     * The maximum timestamp which will be processed in the experiment.
     * Make sure to select a value which is higher than the actual number of tuples covered in the experiment to
     * allow deterministic not periodic policies to compute their look-ahead.
     * The lowest timestamp is assumed to be greater or equal to zero.
     */
    private static final int timeExperimentSize=2000000;

    /**
     * A scale factor: All the minimums and maximums below will be multiplied with this factor.
     */
    private static final double scaleFactor=1000;



    /*******************************************************
     * COUNT BASED QUERIES SETUP
     *******************************************************/

    /**
     * Minimum slide step for the regular case (COUNT)
     */
    private static final double regularCountMinSlide=1;

    /**
     * Maximum slide step for the regular case (COUNT)
     */
    private static final double regularCountMaxSlide=9;

    /**
     * Minimum slide step for the scenario with small slides (COUNT)
     */
    private static final double lowerCountMinSlide=1;

    /**
     * Maximum slide step for the scenario with small slides (COUNT)
     */
    private static final double lowerCountMaxSlide=3;

    /**
     * Minimum slide step for the scenario with large slides (COUNT)
     */
    private static final double upperCountMinSlide=7;

    /**
     * Maximum slide step for the scenario with large slides (COUNT)
     */
    private static final double upperCountMaxSlide=9;

    /**
     * Minimum range for the regular case (COUNT)
     */
    private static final double regularCountMinRange=11;

    /**
     * Maximum range for the regular case (COUNT)
     */
    private static final double regularCountMaxRange=19;

    /**
     * Minimum range for the scenario with small ranges (COUNT)
     */
    private static final double lowerCountMinRange=11;

    /**
     * Maximum range for the scenario with small ranges (COUNT)
     */
    private static final double lowerCountMaxRange=13;

    /**
     * Minimum range for the scenario with large ranges (COUNT)
     */
    private static final double upperCountMinRange=17;

    /**
     * Maximum range for the scenario with large ranges (COUNT)
     */
    private static final double upperCountMaxRange=19;




    /*******************************************************
     * TIME BASED QUERIES SETUP
     *******************************************************/

    /**
     * Minimum slide step for the regular case (TIME)
     */
    private static final double regularTimeMinSlide=1;

    /**
     * Maximum slide step for the regular case (TIME)
     */
    private static final double regularTimeMaxSlide=9;

    /**
     * Minimum slide step for the scenario with small slides (TIME)
     */
    private static final double lowerTimeMinSlide=1;

    /**
     * Maximum slide step for the scenario with small slides (TIME)
     */
    private static final double lowerTimeMaxSlide=3;

    /**
     * Minimum slide step for the scenario with large slides (TIME)
     */
    private static final double upperTimeMinSlide=7;

    /**
     * Maximum slide step for the scenario with large slides (TIME)
     */
    private static final double upperTimeMaxSlide=9;

    /**
     * Minimum range for the regular case (TIME)
     */
    private static final double regularTimeMinRange=11;

    /**
     * Maximum range for the regular case (TIME)
     */
    private static final double regularTimeMaxRange=19;

    /**
     * Minimum range for the scenario with small ranges (TIME)
     */
    private static final double lowerTimeMinRange=11;

    /**
     * Maximum range for the scenario with small ranges (TIME)
     */
    private static final double lowerTimeMaxRange=13;

    /**
     * Minimum range for the scenario with large ranges (TIME)
     */
    private static final double upperTimeMinRange=17;

    /**
     * Maximum range for the scenario with large ranges (TIME)
     */
    private static final double upperTimeMaxRange=19;



    /**
     * An instance of the random class used to create gaussian distributed random values
     */
    private static final Random rnd = new Random();


    /**
     * The main method executes the generation of the test setup.
     * @param args not used
     * @throws Exception Any exception which may occurs when trying to write the output file
     */
    public static void main (String[] args) throws Exception{

        makeSetup(regularCountMinSlide*scaleFactor, regularCountMaxSlide*scaleFactor, lowerCountMinSlide*scaleFactor, lowerCountMaxSlide*scaleFactor, upperCountMinSlide*scaleFactor, upperCountMaxSlide*scaleFactor,
        regularCountMinRange*scaleFactor, regularCountMaxRange*scaleFactor, lowerCountMinRange*scaleFactor, lowerCountMaxRange*scaleFactor, upperCountMinRange*scaleFactor, upperCountMaxRange*scaleFactor,
        regularTimeMinSlide*scaleFactor, regularTimeMaxSlide*scaleFactor, lowerTimeMinSlide*scaleFactor, lowerTimeMaxSlide*scaleFactor, upperTimeMinSlide*scaleFactor, upperTimeMaxSlide*scaleFactor,
        regularTimeMinRange*scaleFactor, regularTimeMaxRange*scaleFactor, lowerTimeMinRange*scaleFactor, lowerTimeMaxRange*scaleFactor, upperTimeMinRange*scaleFactor, upperTimeMaxRange*scaleFactor,
        numTimeQueries, numCountQueries);

    }

    /**
     * Creates the queries setup (all required random values) and writes them to an output file.
     * @param regularCountMinSlide The minimal slide step for count-based queries in the regular scenario
     * @param regularCountMaxSlide The maximal slide step for count-based queries in the regular scenario
     * @param lowerCountMinSlide The minimal slide step for count-based queries in the low-slide scenario
     * @param lowerCountMaxSlide The maximal slide step for count-based queries in the low-slide scenario
     * @param upperCountMinSlide The minimal slide step for count-based queries in the high-slide scenario
     * @param upperCountMaxSlide The maximal slide step for count-based queries in the high-slide scenario
     * @param regularCountMinRange The minimal range for count-based queries in the regular scenario
     * @param regularCountMaxRange The maximal range for count-based queries in the regular scenario
     * @param lowerCountMinRange The minimal range for count-based queries in the low-range scenario
     * @param lowerCountMaxRange The maximal range for count-based queries in the low-range scenario
     * @param upperCountMinRange The minimal range for count-based queries in the high-range scenario
     * @param upperCountMaxRange The maximal range for count-based queries in the high-range scenario
     * @param regularTimeMinSlide The minimal slide step for time-based queries in the regular scenario
     * @param regularTimeMaxSlide The maximal slide step for time-based queries in the regular scenario
     * @param lowerTimeMinSlide The minimal slide step for time-based queries in the low-slide scenario
     * @param lowerTimeMaxSlide The maximal slide step for time-based queries in the low-slide scenario
     * @param upperTimeMinSlide The minimal slide step for time-based queries in the high-slide scenario
     * @param upperTimeMaxSlide The maximal slide step for time-based queries in the high-slide scenario
     * @param regularTimeMinRange The minimal range for time-based queries in the regular scenario
     * @param regularTimeMaxRange The maximal range for time-based queries in the regular scenario
     * @param lowerTimeMinRange The minimal range for time-based queries in the low-range scenario
     * @param lowerTimeMaxRange The maximal range for time-based queries in the low-range scenario
     * @param upperTimeMinRange The minimal range for time-based queries in the high-range scenario
     * @param upperTimeMaxRange The maximal range for time-based queries in the high-range scenario
     * @param numTimeQueries Number of time-based queries
     * @param numCountQueries Number of count-based queries
     * @throws FileNotFoundException If the output file cannot be accessed.
     * @throws UnsupportedEncodingException If no file using UTF8 encoding can be created.
     */
    private static void makeSetup(double regularCountMinSlide, double regularCountMaxSlide, double lowerCountMinSlide, double lowerCountMaxSlide, double upperCountMinSlide, double upperCountMaxSlide,
                                  double regularCountMinRange, double regularCountMaxRange, double lowerCountMinRange, double lowerCountMaxRange, double upperCountMinRange, double upperCountMaxRange,
                                  double regularTimeMinSlide, double regularTimeMaxSlide, double lowerTimeMinSlide, double lowerTimeMaxSlide, double upperTimeMinSlide, double upperTimeMaxSlide,
                                  double regularTimeMinRange, double regularTimeMaxRange, double lowerTimeMinRange, double lowerTimeMaxRange, double upperTimeMinRange, double upperTimeMaxRange,
                                  int numTimeQueries, int numCountQueries) throws FileNotFoundException, UnsupportedEncodingException {

        //make values for count based windows
        double regularCountWindowSlideDefs[]= new double[numCountQueries];
        double regularCountWindowRangeDefs[]= new double[numCountQueries];
        double lowerCountWindowSlideDefs[]  = new double[numCountQueries];
        double lowerCountWindowRangeDefs[]  = new double[numCountQueries];
        double upperCountWindowSlideDefs[]  = new double[numCountQueries];
        double upperCountWindowRangeDefs[]  = new double[numCountQueries];
        for (int i=0; i<numCountQueries;i++){
            regularCountWindowSlideDefs[i] = makeGaussian(regularCountMinSlide, regularCountMaxSlide);
            regularCountWindowRangeDefs[i] = makeGaussian(regularCountMinRange, regularCountMaxRange);
            lowerCountWindowSlideDefs[i]   = makeGaussian(lowerCountMinSlide, lowerCountMaxSlide);
            lowerCountWindowRangeDefs[i]   = makeGaussian(lowerCountMinRange, lowerCountMaxRange);
            upperCountWindowSlideDefs[i]   = makeGaussian(upperCountMinSlide, upperCountMaxSlide);
            upperCountWindowRangeDefs[i]   = makeGaussian(upperCountMinRange, upperCountMaxRange);
        }

        //make values for time based windows
        double regularTimeWindowSlideDefs[]= new double[numTimeQueries];
        double regularTimeWindowRangeDefs[]= new double[numTimeQueries];
        double lowerTimeWindowSlideDefs[]  = new double[numTimeQueries];
        double lowerTimeWindowRangeDefs[]  = new double[numTimeQueries];
        double upperTimeWindowSlideDefs[]  = new double[numTimeQueries];
        double upperTimeWindowRangeDefs[]  = new double[numTimeQueries];
        for (int i=0; i<numTimeQueries;i++){
            regularTimeWindowSlideDefs[i] = makeGaussian(regularTimeMinSlide, regularTimeMaxSlide);
            regularTimeWindowRangeDefs[i] = makeGaussian(regularTimeMinRange, regularTimeMaxRange);
            lowerTimeWindowSlideDefs[i]   = makeGaussian(lowerTimeMinSlide, lowerTimeMaxSlide);
            lowerTimeWindowRangeDefs[i]   = makeGaussian(lowerTimeMinRange, lowerTimeMaxRange);
            upperTimeWindowSlideDefs[i]   = makeGaussian(upperTimeMinSlide, upperTimeMaxSlide);
            upperTimeWindowRangeDefs[i]   = makeGaussian(upperTimeMinRange, upperTimeMaxRange);
        }

        //Write out windows for the 6 scenarios (Full setup)
        PrintWriter writer = new PrintWriter("test-setup.txt", "UTF-8");
        //Write out ranges and slides for plotting them
        PrintWriter plotWriter1 = new PrintWriter("plot-range-slide1.txt", "UTF-8");

        //Regular scenario
        writer.println("SCENARIO 1: REGULAR RANGE; REGULAR SLIDE");
        writer.println("\tDETERMINISTIC AND PERIODIC");
        for (int i=0;i<numCountQueries;i++){
            writer.println("\t\tCOUNT\t"+regularCountWindowRangeDefs[i]+"\t"+regularCountWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
            plotWriter1.println(regularCountWindowRangeDefs[i]+"\t"+regularCountWindowSlideDefs[i]+"\ta");
        }
        for (int i=0;i<numTimeQueries;i++){
            writer.println("\t\tTIME\t"+regularTimeWindowRangeDefs[i]+"\t"+regularTimeWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
        }

        //Slide Regular, Range low
        writer.println("SCENARIO 2: LOW RANGE; REGULAR SLIDE");
        writer.println("\tDETERMINISTIC AND PERIODIC");
        for (int i=0;i<numCountQueries;i++){
            writer.println("\t\tCOUNT\t"+lowerCountWindowRangeDefs[i]+"\t"+regularCountWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
            plotWriter1.println(lowerCountWindowRangeDefs[i]+"\t"+regularCountWindowSlideDefs[i]+"\tb");
        }
        for (int i=0;i<numTimeQueries;i++){
            writer.println("\t\tTIME\t"+lowerTimeWindowRangeDefs[i]+"\t"+regularTimeWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
        }

        //Slide Regular, High range
        writer.println("SCENARIO 3: HIGH RANGE; REGULAR SLIDE");
        writer.println("\tDETERMINISTIC AND PERIODIC");
        for (int i=0;i<numCountQueries;i++){
            writer.println("\t\tCOUNT\t"+upperCountWindowRangeDefs[i]+"\t"+regularCountWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
            plotWriter1.println(upperCountWindowRangeDefs[i]+"\t"+regularCountWindowSlideDefs[i]+"\tc");
        }
        for (int i=0;i<numTimeQueries;i++){
            writer.println("\t\tTIME\t"+upperTimeWindowRangeDefs[i]+"\t"+regularTimeWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
        }

        //Slide low, Range regular
        writer.println("SCENARIO 4: REGULAR RANGE; LOW SLIDE");
        writer.println("\tDETERMINISTIC AND PERIODIC");
        for (int i=0;i<numCountQueries;i++){
            writer.println("\t\tCOUNT\t"+regularCountWindowRangeDefs[i]+"\t"+lowerCountWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
            plotWriter1.println(regularCountWindowRangeDefs[i]+"\t"+lowerCountWindowSlideDefs[i]+"\td");
        }
        for (int i=0;i<numTimeQueries;i++){
            writer.println("\t\tTIME\t"+regularTimeWindowRangeDefs[i]+"\t"+lowerTimeWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
        }

        //Slide high, Range regular
        writer.println("SCENARIO 5: REGULAR RANGE; HIGH SLIDE");
        writer.println("\tDETERMINISTIC AND PERIODIC");
        for (int i=0;i<numCountQueries;i++){
            writer.println("\t\tCOUNT\t"+regularCountWindowRangeDefs[i]+"\t"+upperCountWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
            plotWriter1.println(regularCountWindowRangeDefs[i]+"\t"+upperCountWindowSlideDefs[i]+"\te");
        }
        for (int i=0;i<numTimeQueries;i++){
            writer.println("\t\tTIME\t"+regularTimeWindowRangeDefs[i]+"\t"+upperTimeWindowSlideDefs[i]+"\t"+(10000/regularCountWindowSlideDefs[i]));
        }

        //Random Walk cases:
        //Generate randomly places window ends per query:

        //Regular scenario
        writer.println("SCENARIO 1: REGULAR RANGE; REGULAR SLIDE");
        writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+regularCountWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(countExperimentSize,regularCountWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+regularTimeWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(timeExperimentSize,regularTimeWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }

        writer.println("SCENARIO 2: LOW RANGE; REGULAR SLIDE");
        writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
        //Slide Regular, Range low
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+lowerCountWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(countExperimentSize,regularCountWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+lowerTimeWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(timeExperimentSize,regularTimeWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }

        writer.println("SCENARIO 1: HIGH RANGE; REGULAR SLIDE");
        writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
        //Slide Regular, High range
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+upperCountWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(countExperimentSize,regularCountWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+upperTimeWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(timeExperimentSize,regularTimeWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }

        writer.println("SCENARIO 1: REGULAR RANGE; LOW SLIDE");
        writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
        //Slide low, Range regular
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+regularCountWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(countExperimentSize,lowerCountWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+regularTimeWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(timeExperimentSize,lowerTimeWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }

        writer.println("SCENARIO 1: REGULAR RANGE; HIGH SLIDE");
        writer.println("\tRANDOM WINDOW ENDS (DETERMINISTIC BUT NOT PERIODIC)");
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+regularCountWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(countExperimentSize,upperCountWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }
        for (int i=0;i<numCountQueries;i++){
            writer.print("\t\tCOUNT\t"+regularTimeWindowRangeDefs[i]+"\t");
            for (Double d:makeRandomValueList(timeExperimentSize,upperTimeWindowSlideDefs[i])){
                writer.print(d+" ");
            }
            writer.println();
        }

        writer.flush();
        writer.close();

        plotWriter1.flush();
        plotWriter1.close();
    }

    /**
     * Creates an ordered list of random values reaching from 0 to maxValue.
     * Exactly maxValue/slideSize random values are generated.
     * @param maxValue the maximal possible return window
     * @param slideSize the slide size of the window (used to calculate the required number of random values)
     * @return A ordered list of random values reaching from 0 to maxValue.
     */
    private static Double[] makeRandomValueList(int maxValue,double slideSize){
        LinkedList<Double> values=new LinkedList<Double>();

        for (int i=0; i<=maxValue/slideSize;i++){
            double value=0;
            while (value<1){
                value=rnd.nextDouble()*maxValue;
            }
            values.add(value);
        }

        Collections.sort(values,new Comparator<Double>() {
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
     * @param min the minimal possible return value
     * @param max the maximal possible return value
     * @return a random value between min and max (gaussion distributed)
     */
    private static double makeGaussian(double min,double max){
        double value;
        double sdv=(max-min)/7;
        double mean=min+((max-min)/2);
        while (true){
            if ((value=rnd.nextGaussian() * sdv + mean) >= min && value <= max){
                return value;
            }
        }
    }

}
