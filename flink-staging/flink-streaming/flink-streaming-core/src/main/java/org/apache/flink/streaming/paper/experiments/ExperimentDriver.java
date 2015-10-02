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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.*;
import org.apache.flink.streaming.api.windowing.windowbuffer.AggregationStats;
import org.apache.flink.streaming.paper.AggregationUtils;
import org.apache.flink.streaming.paper.PaperExperiment;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.streaming.paper.AggregationUtils.SumAggregation;

/**
 * This class reads an experiment setup generated by @link{DataGenerator} from a file and executes the experiments.
 * The results are written to a file.
 */
public class ExperimentDriver {

    /**
     * Specify below which cases you want to run
     */
    private static final boolean RUN_PAIRS_LAZY =false;
    private static final boolean RUN_PAIRS_EAGER =false;
    private static final boolean RUN_PERIODIC=true;
    private static final boolean RUN_DETERMINISTIC_NOT_PERIODIC=true;
    private static final boolean RUN_NOT_DETERMINISTIC_NOT_PERIODIC=true;
    private static final boolean RUN_PERIODIC_NO_PREAGG=true;
    private static final boolean RUN_PERIODIC_EAGER=true;
    private static final boolean RUN_DETERMINISTIC_NOT_PERIODIC_EAGER=true;
    private static final boolean RUN_NOT_DETERMINISTIC_NOT_PERIODIC_EAGER=true;
    private static final boolean RUN_PERIODIC_NO_PREAGG_EAGER=true;

    /**
     * Specify the output file for the experiment results
     */
    private static String SETUP_PATH = "test-setup.txt";
    private static String RESULT_PATH = "test-result.txt";

    /**
     * Specify the number of tuples you want to process
     */
    private static final int NUM_TUPLES = 1000000;

    /**
     * Set a sleep period in ms.
     * The data source will pause between input emissions for the given time.
     */
    private static final int SOURCE_SLEEP = 0;

    /**
     * Main program: Runs all the test cases and writed the results to the specified output files.
     * @param args not used,
     * @throws Exception Any exception which may occurs at the runtime.
     */
    public static void main(String[] args) throws Exception {

        //If setup and result paths are provided as paramaters, replace the defaults
        if (args.length==2){
            SETUP_PATH=args[0];
            RESULT_PATH=args[1];
        }

        //Read the setup
        BufferedReader br = new BufferedReader(new FileReader(SETUP_PATH));
        String line = null;
        AggregationStats stats=AggregationStats.getInstance();

        //Read the periodic setups
        @SuppressWarnings("unchecked")
        LinkedList<Tuple3<String, Double, Double>>[] scenario = new LinkedList[9];
        line=br.readLine();
        for (int i = 0; i < 9; i++) {
            scenario[i] = new LinkedList<Tuple3<String, Double, Double>>();
            //Skip separation lines between scenarios
            while (line!=null &&(!line.startsWith("SCENARIO "+(i+1)))){
                i++;
                scenario[i] = new LinkedList<Tuple3<String, Double, Double>>();
            }
            do {
                line = br.readLine();
            } while (!line.startsWith("\t\t"));
            //parse data rows
            do {
                String[] fields = line.split("\t");
                scenario[i].add(new Tuple3<String, Double, Double>(fields[2], Double.parseDouble(fields[3]), Double.parseDouble(fields[4])));
                line = br.readLine();
            } while (line != null && line.startsWith("\t\t"));
        }

        //Read the random walk setup
        @SuppressWarnings("unchecked")
        LinkedList<Tuple3<String, Double, Double[]>>[] randomScenario = new LinkedList[9];
        if (line==null){
            line=br.readLine();
        }
        for (int i = 0; i < 9; i++) {
            randomScenario[i] = new LinkedList<Tuple3<String, Double, Double[]>>();
            //Skip separation lines between scenarios
            while (line!=null &&(! line.startsWith("SCENARIO "+(i+1)))){
                i++;
                randomScenario[i] = new LinkedList<Tuple3<String, Double, Double[]>>();
            }

            //Stop loop if eof is reached (happens if there is no random walk setup present)
            if (line == null){
                break;
            }

            do {
                line = br.readLine();
            } while (!line.startsWith("\t\t"));
            //parse data rows
            do {
                String[] fields = line.split("\t");
                String[] windowEndsString = fields[4].split(" ");
                Double[] windowEnds = new Double[windowEndsString.length];
                for (int j = 0; j < windowEndsString.length; j++) {
                    windowEnds[j] = Double.parseDouble(windowEndsString[j]);
                }
                randomScenario[i].add(new Tuple3<String, Double, Double[]>(fields[2], Double.parseDouble(fields[3]), windowEnds));
                line = br.readLine();
            } while (line != null && line.startsWith("\t\t"));
        }

        //Writer for the results
        PrintWriter resultWriter = new PrintWriter(RESULT_PATH, "UTF-8");
        resultWriter.println("SCEN\tCASE\tTIME\tAGG\tRED\tUPD\tMAXB\tAVGB\tUPD_AVG\tUPD_CNT\tMERGE_AVG\tMERGE_CNT");

        //run simple program to warm up (The first start up takes more time...)
        runWarmUpTask();

        //Variables needed in the loop
        JobExecutionResult result;

        //Run experiments for different scenarios
        // Scenario 1 (i=0): regular range, regular slide
        // Scenario 2 (i=1): low range, regular slide
        // Scenario 3 (i=2): high range, regular slide
        // Scenario 4 (i=3): regular range, low slide
        // Scenario 5 (i=4): regular range, high slide
        // Scenario 6 (i=5): high range, high slide
        // Scenario 7 (i=6): low range, high slide
        // Scenario 8 (i=7): low range, low slide
        // Scenario 9 (i=8): high range, low slide
        //
        //The differen Algorithms are the following
        // CASE 0: Pairs LAZY - original
        // CASE 1: deterministic policy groups; periodic; LAZY
        // CASE 2: deterministic policy groups; not periodic; LAZY
        // CASE 3: not deterministic; not periodic; LAZY
        // CASE 4: not deterministic; periodic; LAZY (theoretically periodic & deterministic, but periodicity is not utilized)
        // CASE 5: Same as case 0 but EAGER
        // CASE 6: Same as case 1 but EAGER
        // CASE 7: Same as case 2 but EAGER
        // CASE 8: Same as case 3 but EAGER
        // CASE 9: Same as case 4 but EAGER
        for (int i = 0; i < 9; i++) {
            //check if the scenario is present in the setup file, otherwise skip this iterations
            if (scenario[i]==null || scenario[i].size()==0){
                continue;
            }

            int testCase=0;

            if (RUN_PAIRS_LAZY){

                StreamExecutionEnvironment env0 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source2 = env0.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

                List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> periodicPolicyGroups = makePeriodicPolicyGroups(scenario[i]);
                SumAggregation.applyOn(source2, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                                List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                                List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(periodicPolicyGroups, new LinkedList<TriggerPolicy<Tuple3<Double, Double, Long>>>(),
                                new LinkedList<EvictionPolicy<Tuple3<Double, Double, Long>>>()), AggregationUtils.AGGREGATION_TYPE.LAZY,
                        AggregationUtils.DISCRETIZATION_TYPE.PAIRS)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-"+i+"-"+testCase, FileSystem.WriteMode.OVERWRITE);

                result = env0.execute("Scanario "+i+" Case "+testCase);

                finalizeExperiment(stats,resultWriter, result, i, testCase);
            }

            testCase++;

            if (RUN_PERIODIC){

                StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source2 = env2.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

                List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> periodicPolicyGroups = makePeriodicPolicyGroups(scenario[i]);
                SumAggregation.applyOn(source2, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                        List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                        List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(periodicPolicyGroups, new LinkedList<TriggerPolicy<Tuple3<Double, Double, Long>>>(),
                        new LinkedList<EvictionPolicy<Tuple3<Double, Double, Long>>>()), AggregationUtils.AGGREGATION_TYPE.LAZY, 
                        AggregationUtils.DISCRETIZATION_TYPE.B2B)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env2.execute("Scanario "+i+" Case "+testCase);

                finalizeExperiment(stats,resultWriter, result, i, testCase);
            }

            testCase++;

            if (RUN_DETERMINISTIC_NOT_PERIODIC && randomScenario[i]!=null && randomScenario[i].size()>0){
                /*
                 * Evaluate with deterministic policy groups (not periodic)  (case 3)
                 */

                StreamExecutionEnvironment env3 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source3 = env3.addSource(new DataGenerator(SOURCE_SLEEP,NUM_TUPLES));

                List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> randomWalkPolicyGroups = makeRandomWalkPolicyGroups(randomScenario[i]);
                SumAggregation.applyOn(source3, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                        List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                        List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(randomWalkPolicyGroups, new LinkedList<TriggerPolicy<Tuple3<Double, Double, Long>>>(),
                        new LinkedList<EvictionPolicy<Tuple3<Double, Double, Long>>>()), AggregationUtils.AGGREGATION_TYPE.LAZY,
                        AggregationUtils.DISCRETIZATION_TYPE.B2B)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env3.execute();

                finalizeExperiment(stats,resultWriter, result, i, testCase);
            }

            testCase++;




            if (RUN_NOT_DETERMINISTIC_NOT_PERIODIC && randomScenario[i]!=null && randomScenario[i].size()>0){
                /*
                 *Evaluate not deterministic version  (case 4)
                 */

                StreamExecutionEnvironment env4 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source4 = env4.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

                SumAggregation.applyOn(source4, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                        List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                        List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(new LinkedList<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>(), makeNDRandomWalkTrigger(randomScenario[i]),
                        makeNDRandomWalkEviction(randomScenario[i])), AggregationUtils.AGGREGATION_TYPE.LAZY,
                        AggregationUtils.DISCRETIZATION_TYPE.B2B)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env4.execute();

                finalizeExperiment(stats,resultWriter, result, i, testCase);
            }

            testCase++;

            if (RUN_PERIODIC_NO_PREAGG){
                /*
                 *Evaluate periodic setup, but without any pre-aggregation (case 5)
                 */

                StreamExecutionEnvironment env5 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source4 = env5.addSource(new DataGenerator(SOURCE_SLEEP,NUM_TUPLES));

                SumAggregation.applyOn(source4, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                        List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                        List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(new LinkedList<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>(), makeNDPeriodicTrigger(scenario[i]),
                        makeNDPeriodicEviction(scenario[i])), AggregationUtils.AGGREGATION_TYPE.LAZY, AggregationUtils.DISCRETIZATION_TYPE.B2B)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env5.execute();

                finalizeExperiment(stats,resultWriter, result, i, testCase);
            }

            testCase++;
            
            if (RUN_PAIRS_EAGER){
                                /*
                 * Evaluate with deterministic policy groups (periodic) EAGER version  (case 6)
                 */

                StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source2 = env1.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

                List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> periodicPolicyGroups = makePeriodicPolicyGroups(scenario[i]);
                SumAggregation.applyOn(source2, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                                List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                                List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(periodicPolicyGroups, new LinkedList<TriggerPolicy<Tuple3<Double, Double, Long>>>(),
                                new LinkedList<EvictionPolicy<Tuple3<Double, Double, Long>>>()), AggregationUtils.AGGREGATION_TYPE.EAGER,
                        AggregationUtils.DISCRETIZATION_TYPE.PAIRS)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env1.execute("Scanario "+i+" Case "+testCase);

                finalizeExperiment(stats,resultWriter, result, i, testCase);
            }

            testCase++;
            
            if (RUN_PERIODIC_EAGER){
                /*
                 * Evaluate with deterministic policy groups (periodic) EAGER version  (case 6)
                 */

                StreamExecutionEnvironment env6 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source2 = env6.addSource(new DataGenerator(SOURCE_SLEEP, NUM_TUPLES));

                List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> periodicPolicyGroups = makePeriodicPolicyGroups(scenario[i]);
                SumAggregation.applyOn(source2, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                        List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                        List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(periodicPolicyGroups, new LinkedList<TriggerPolicy<Tuple3<Double, Double, Long>>>(),
                        new LinkedList<EvictionPolicy<Tuple3<Double, Double, Long>>>()), AggregationUtils.AGGREGATION_TYPE.EAGER,
                        AggregationUtils.DISCRETIZATION_TYPE.B2B)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env6.execute("Scanario "+i+" Case "+testCase);

                finalizeExperiment(stats, resultWriter, result, i, testCase);
            }

            testCase++;

            if (RUN_DETERMINISTIC_NOT_PERIODIC_EAGER && randomScenario[i]!=null && randomScenario[i].size()>0){
                /*
                 * Evaluate with deterministic policy groups (not periodic) EAGER version (case 7)
                 */

                StreamExecutionEnvironment env7 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source3 = env7.addSource(new DataGenerator(SOURCE_SLEEP,NUM_TUPLES));

                List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> randomWalkPolicyGroups = makeRandomWalkPolicyGroups(randomScenario[i]);
                SumAggregation.applyOn(source3, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                        List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                        List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(randomWalkPolicyGroups, new LinkedList<TriggerPolicy<Tuple3<Double, Double, Long>>>(),
                        new LinkedList<EvictionPolicy<Tuple3<Double, Double, Long>>>()), AggregationUtils.AGGREGATION_TYPE.EAGER,
                        AggregationUtils.DISCRETIZATION_TYPE.B2B)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env7.execute();

                finalizeExperiment(stats, resultWriter, result, i, testCase);
            }

            testCase++;

            if (RUN_NOT_DETERMINISTIC_NOT_PERIODIC_EAGER && randomScenario[i]!=null && randomScenario[i].size()>0){
                /*
                 *Evaluate not deterministic version  (case 8)
                 */

                StreamExecutionEnvironment env4 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source4 = env4.addSource(new DataGenerator(SOURCE_SLEEP,NUM_TUPLES));

                SumAggregation.applyOn(source4, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                        List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                        List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(new LinkedList<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>(), makeNDRandomWalkTrigger(randomScenario[i]),
                        makeNDRandomWalkEviction(randomScenario[i])), AggregationUtils.AGGREGATION_TYPE.EAGER,
                        AggregationUtils.DISCRETIZATION_TYPE.B2B)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env4.execute();

                finalizeExperiment(stats, resultWriter, result, i, testCase);
            }

            testCase++;

            if (RUN_PERIODIC_NO_PREAGG_EAGER){
                /*
                 *Evaluate periodic setup, but without any pre-aggregation (case 9)
                 */

                StreamExecutionEnvironment env5 = StreamExecutionEnvironment.createLocalEnvironment(1);
                DataStream<Tuple3<Double, Double, Long>> source4 = env5.addSource(new DataGenerator(SOURCE_SLEEP,NUM_TUPLES));

                SumAggregation.applyOn(source4, new Tuple3<List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>,
                        List<TriggerPolicy<Tuple3<Double, Double, Long>>>,
                        List<EvictionPolicy<Tuple3<Double, Double, Long>>>>(new LinkedList<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>(), makeNDPeriodicTrigger(scenario[i]),
                        makeNDPeriodicEviction(scenario[i])), AggregationUtils.AGGREGATION_TYPE.EAGER,
                        AggregationUtils.DISCRETIZATION_TYPE.B2B)
                        .map(new PaperExperiment.Prefix("SUM")).writeAsText("result-" + i + "-" + testCase, FileSystem.WriteMode.OVERWRITE);

                result = env5.execute();

                finalizeExperiment(stats, resultWriter, result, i, testCase);
            }

        }

        //close writer
        resultWriter.flush();
        resultWriter.close();
    }

    private static void finalizeExperiment(AggregationStats stats, PrintWriter resultWriter, JobExecutionResult result, int scenarioId, int caseId) {
        resultWriter.println(scenarioId + "\t" + caseId + "\t" + result.getNetRuntime()+ "\t" +stats.getAggregateCount()
                + "\t" +stats.getReduceCount()+ "\t" + stats.getUpdateCount() + "\t" + stats.getMaxBufferSize() + "\t" + stats.getAverageBufferSize() 
                + "\t" + stats.getAverageUpdTime() + "\t" + stats.getTotalUpdateCount()+ "\t" + stats.getAverageMergeTime()+ "\t" + stats.getTotalMergeCount());
        stats.reset();
        resultWriter.flush();
    }

    @SuppressWarnings("unchecked")
    public static List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> makePeriodicPolicyGroups(LinkedList<Tuple3<String, Double, Double>> settings) {

        List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> policyGroups = new LinkedList<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>();
        for (Tuple3<String, Double, Double> setting : settings) {
            DeterministicTriggerPolicy<Tuple3<Double, Double, Long>> trigger;
            DeterministicEvictionPolicy<Tuple3<Double, Double, Long>> eviction;
            if (setting.f0.equals("COUNT")) {
                trigger = new DeterministicCountTriggerPolicy<Tuple3<Double, Double, Long>>(setting.f2.intValue());
                eviction = new DeterministicCountEvictionPolicy<Tuple3<Double, Double, Long>>(setting.f1.intValue());
                policyGroups.add(new DeterministicPolicyGroup<Tuple3<Double, Double, Long>>(trigger, eviction, countExtractor));
            } else {
                trigger = new DeterministicTimeTriggerPolicy<Tuple3<Double, Double, Long>>(setting.f2.intValue(), timestampWrapper);
                eviction = new DeterministicTimeEvictionPolicy<Tuple3<Double, Double, Long>>(setting.f1.intValue(), timestampWrapper);
                policyGroups.add(new DeterministicPolicyGroup<Tuple3<Double, Double, Long>>(trigger, eviction, timeExtractor));
            }
        }

        return policyGroups;
    }

    @SuppressWarnings("unchecked")
    public static List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> makeRandomWalkPolicyGroups(List<Tuple3<String, Double, Double[]>> settings) {

        List<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>> policyGroups = new LinkedList<DeterministicPolicyGroup<Tuple3<Double, Double, Long>>>();
        for (Tuple3<String, Double, Double[]> setting : settings) {
            DeterministicTriggerPolicy<Tuple3<Double, Double, Long>> trigger;
            DeterministicEvictionPolicy<Tuple3<Double, Double, Long>> eviction;
            if (setting.f0.equals("COUNT")) {
                trigger = new DeterministicRandomWalkTriggerPolicy<Tuple3<Double, Double, Long>>(setting.f2, countExtractor);
                eviction = new DeterministicCountEvictionPolicy<Tuple3<Double, Double, Long>>(setting.f1.intValue());
                policyGroups.add(new DeterministicPolicyGroup<Tuple3<Double, Double, Long>>(trigger, eviction, countExtractor));
            } else {
                trigger = new DeterministicRandomWalkTriggerPolicy<Tuple3<Double, Double, Long>>(setting.f2, timeExtractor);
                eviction = new DeterministicTimeEvictionPolicy<Tuple3<Double, Double, Long>>(setting.f1.intValue(), timestampWrapper);
                policyGroups.add(new DeterministicPolicyGroup<Tuple3<Double, Double, Long>>(trigger, eviction, timeExtractor));
            }
        }

        return policyGroups;
    }

    public static List<TriggerPolicy<Tuple3<Double, Double, Long>>> makeNDRandomWalkTrigger(List<Tuple3<String, Double, Double[]>> settings){
        List<TriggerPolicy<Tuple3<Double, Double, Long>>> trigger = new LinkedList<TriggerPolicy<Tuple3<Double, Double, Long>>>();
        for (Tuple3<String, Double, Double[]> setting : settings) {
            if (setting.f0.equals("COUNT")) {
                trigger.add(new RandomWalkTriggerPolicy<Tuple3<Double, Double, Long>>(setting.f2,countExtractor));
            } else {
                trigger.add(new RandomWalkTriggerPolicy<Tuple3<Double, Double, Long>>(setting.f2,timeExtractor));
            }
        }
        return trigger;
    }

    @SuppressWarnings("unchecked")
    public static List<EvictionPolicy<Tuple3<Double, Double, Long>>> makeNDRandomWalkEviction (List<Tuple3<String, Double, Double[]>> settings){
        List<EvictionPolicy<Tuple3<Double, Double, Long>>> eviction = new LinkedList<EvictionPolicy<Tuple3<Double, Double, Long>>>();
        for (Tuple3<String, Double, Double[]> setting : settings) {
            if (setting.f0.equals("COUNT")) {
                 eviction.add(new CountEvictionPolicy<Tuple3<Double, Double, Long>>(setting.f1.intValue()));
            } else {
                 eviction.add(new TimeEvictionPolicy<Tuple3<Double, Double, Long>>(setting.f1.intValue(),timestampWrapper));
            }
        }
        return eviction;
    }

    @SuppressWarnings("unchecked")
    public static List<TriggerPolicy<Tuple3<Double, Double, Long>>> makeNDPeriodicTrigger(List<Tuple3<String, Double, Double>> settings){
        List<TriggerPolicy<Tuple3<Double, Double, Long>>> trigger = new LinkedList<TriggerPolicy<Tuple3<Double, Double, Long>>>();
        for (Tuple3<String, Double, Double> setting : settings) {
            if (setting.f0.equals("COUNT")) {
                trigger.add(new CountTriggerPolicy<Tuple3<Double, Double, Long>>(setting.f2.intValue()));
            } else {
                trigger.add(new TimeTriggerPolicy<Tuple3<Double, Double, Long>>(setting.f2.intValue(), timestampWrapper));
            }
        }
        return trigger;
    }

    @SuppressWarnings("unchecked")
    public static List<EvictionPolicy<Tuple3<Double, Double, Long>>> makeNDPeriodicEviction (List<Tuple3<String, Double, Double>> settings){
        List<EvictionPolicy<Tuple3<Double, Double, Long>>> eviction = new LinkedList<EvictionPolicy<Tuple3<Double, Double, Long>>>();
        for (Tuple3<String, Double, Double> setting : settings) {
            if (setting.f0.equals("COUNT")) {
                eviction.add(new DeterministicCountEvictionPolicy<Tuple3<Double, Double, Long>>(setting.f1.intValue()));
            } else {
                eviction.add(new DeterministicTimeEvictionPolicy<Tuple3<Double, Double, Long>>(setting.f1.intValue(), timestampWrapper));
            }
        }
        return eviction;
    }

    /**
     * Runs a small warm up job. This is required because the first job needs longer to start up.
     * @throws Exception Any exception which might happens during the execution
     */
    public static void runWarmUpTask() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        DataStream<Tuple3<Double, Double, Long>> source = env.addSource(new DataGenerator(10, 10));
        source.map(new MapFunction<Tuple3<Double, Double, Long>, Long>() {
            @Override
            public Long map(Tuple3<Double, Double, Long> value) throws Exception {
                return value.f2;
            }
        }).print();
        env.execute();
    }

    /**
     * Extracts the current tuple count from the input tuple
     */
    static Extractor<Tuple3<Double, Double, Long>, Double> countExtractor = new Extractor<Tuple3<Double, Double, Long>, Double>() {
        @Override
        public Double extract(Tuple3<Double, Double, Long> in) {
            return in.f1;
        }
    };

    /**
     * Extracts the timestamp from the input tuple (not the system timestamp)
     */
    static Extractor<Tuple3<Double, Double, Long>, Double> timeExtractor = new Extractor<Tuple3<Double, Double, Long>, Double>() {
        @Override
        public Double extract(Tuple3<Double, Double, Long> in) {
            return in.f2.doubleValue();
        }
    };

    @SuppressWarnings("unchecked")
    static TimestampWrapper timestampWrapper = new TimestampWrapper(new Timestamp<Tuple3<Double, Double, Long>>() {

        @Override
        public long getTimestamp(Tuple3<Double, Double, Long> value) {
            return value.f2;
        }
    }, 0);

    /**
     * The data source for all the experiments
     */
    public static class DataGenerator implements
            SourceFunction<Tuple3<Double, Double, Long>> {

        private static final long serialVersionUID = 1L;

        volatile boolean isRunning = false;
        Random rnd;
        int sleepMillis;
        int numTuples;
        double c = 1;
        long time = 0;

        public DataGenerator(int sleepMillis, int numTuples) {
            this.sleepMillis = sleepMillis;
            this.numTuples = numTuples;
        }

        @Override
        public void run(SourceContext<Tuple3<Double, Double, Long>> ctx)
                throws Exception {
            isRunning = true;
            rnd = new Random();
            int i = 0;
            while (isRunning && i++ < numTuples) {
                ctx.collect(new Tuple3<Double, Double, Long>((double) rnd.nextInt(1000), c++, time++));
                Thread.sleep(sleepMillis);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

    }

}