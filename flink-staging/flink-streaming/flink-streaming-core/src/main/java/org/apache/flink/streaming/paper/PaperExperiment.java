package org.apache.flink.streaming.paper;

import java.util.LinkedList;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicCountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.DeterministicTriggerPolicy;

import static org.apache.flink.streaming.paper.AggregationUtils.*;

@SuppressWarnings("serial")
public class PaperExperiment {

	@SuppressWarnings({"unchecked","rawtypes"})
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<Double, Double>> source = env
				.addSource(new DataGenerator(1000));

		DeterministicTriggerPolicy<Tuple2<Double, Double>> trigger = new DeterministicCountTriggerPolicy<Tuple2<Double, Double>>(
				5);
		DeterministicEvictionPolicy<Tuple2<Double, Double>> evictor = new DeterministicCountEvictionPolicy<Tuple2<Double, Double>>(
				10);
		DeterministicPolicyGroup<Tuple2<Double, Double>> group = new DeterministicPolicyGroup<Tuple2<Double, Double>>(
				trigger, evictor, extractor);

		LinkedList<DeterministicPolicyGroup> deterministicGroups = new LinkedList();
		deterministicGroups.add(group);

		SumAggregation.applyOn(source, deterministicGroups, emptyList, emptyList).map(new Mark("SUM")).print();
		StdAggregation.applyOn(source, deterministicGroups, emptyList, emptyList).map(new Mark("STD")).print();


		env.execute();

	}

	@SuppressWarnings("rawtypes")
	static LinkedList emptyList = new LinkedList();

	
	static Extractor<Tuple2<Double, Double>, Double> extractor = new Extractor<Tuple2<Double, Double>, Double>() {
		@Override
		public Double extract(Tuple2<Double, Double> in) {
			return in.f1;
		}
	};
	
	public static class Mark implements MapFunction<Double, String> {

		private String s;

		public Mark(String s) {
			this.s = s;
		}

		@Override
		public String map(Double value) throws Exception {
			return s + " - " + value;
		}

	}

	public static class DataGenerator implements
			SourceFunction<Tuple2<Double, Double>> {

		private static final long serialVersionUID = 1L;

		volatile boolean isRunning = false;
		Random rnd;
		int sleepMillis;
		double c = 1;

		public DataGenerator(int sleepMillis) {
			this.sleepMillis = sleepMillis;
		}

		@Override
		public void run(SourceContext<Tuple2<Double, Double>> ctx)
				throws Exception {
			isRunning = true;
			rnd = new Random();
			while (isRunning) {
				ctx.collect(new Tuple2<Double, Double>((double) rnd
						.nextInt(1000), c++));
				Thread.sleep(sleepMillis);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

	}

}
