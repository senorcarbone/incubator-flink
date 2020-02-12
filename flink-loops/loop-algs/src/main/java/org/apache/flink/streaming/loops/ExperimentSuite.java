package org.apache.flink.streaming.loops;

import org.apache.flink.streaming.loops.bench.BenchmarkConfig;
import org.apache.flink.streaming.loops.graph.LoopExperiment;
import org.apache.flink.streaming.loops.graph.StreamingPageRank;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Enumeration;

public class ExperimentSuite {
	
	public static void run(LoopExperiment exp) throws Exception {
		exp.execute();
	}
	
	public static Logger getLoggerByName(String theOne){
		Enumeration loggers = LogManager.getCurrentLoggers();
		while(loggers.hasMoreElements()){
			Logger logger = (Logger) loggers.nextElement();
			if( logger.getName().equals(theOne))
				return logger;
		}
		return null;
	}
	
	public static LoopExperiment experimentFromConfig(BenchmarkConfig benchConfig) throws Exception {
		switch((String)benchConfig.getParam("bench.algorithm")){
			case "StreamingPageRank":  return new StreamingPageRank(benchConfig);
			default: throw new IllegalArgumentException("please set a valid bench.algorithm");
		}
	}

	public static void main(String[] args) {
		try {
			ExperimentSuite.run(ExperimentSuite.experimentFromConfig((args.length > 0) 
				? new BenchmarkConfig(args[0]) 
				: new BenchmarkConfig()));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}
