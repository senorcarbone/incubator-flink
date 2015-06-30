package org.apache.flink.streaming.api.windowing;

import java.util.LinkedList;
import java.util.Random;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.windowing.MultiDiscretizer;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTriggerPolicy;

public class PaperExperiment {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(4);

		DataStream<Integer> source = env.addSource(new DataGenerator(1000));

		discretize(source).print();

		env.execute();

	}

	public static DataStream<Tuple2<Integer, Integer>> discretize(
			DataStream<Integer> input) {
		return input.transform("Multidiscretizer", TypeInfoParser
				.<Tuple2<Integer, Integer>> parse("Tuple2<Integer,Integer>"),
				getDiscretizer());
	}

	public static MultiDiscretizer<Integer> getDiscretizer() {

		DeterministicTriggerPolicy<Integer> trigger = new DeterministicCountTriggerPolicy<Integer>(
				5);
		DeterministicEvictionPolicy<Integer> evictor = new DeterministicCountEvictionPolicy<Integer>(
				10);
		DeterministicPolicyGroup<Integer> group = new DeterministicPolicyGroup<Integer>(
				trigger, evictor, countExtractor);

		LinkedList<DeterministicPolicyGroup<Integer>> deterministicGroups = new LinkedList<DeterministicPolicyGroup<Integer>>();
		deterministicGroups.add(group);

		return new MultiDiscretizer<Integer>(deterministicGroups, emptyList, emptyList, sumReducer);
	}
	
	static LinkedList emptyList = new LinkedList();

	static Extractor<Integer, Double> countExtractor = new Extractor<Integer, Double>() {

		Double count = 0.;

		@Override
		public Double extract(Integer in) {
			return ++count;
		}
	};

	static ReduceFunction<Integer> sumReducer = new ReduceFunction<Integer>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}

	};

	public static class DataGenerator implements
			ParallelSourceFunction<Integer> {

		private static final long serialVersionUID = 1L;

		volatile boolean isRunning = false;
		Random rnd;
		int sleepMillis;

		public DataGenerator(int sleepMillis) {
			this.sleepMillis = sleepMillis;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			isRunning = true;
			rnd = new Random();
			while (isRunning) {
				ctx.collect(rnd.nextInt(1000));
				Thread.sleep(sleepMillis);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

	}

}
