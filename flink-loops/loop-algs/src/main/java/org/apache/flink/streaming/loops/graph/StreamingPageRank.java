package org.apache.flink.streaming.loops.graph;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.FeedbackBuilder;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.loops.ExperimentSuite;
import org.apache.flink.streaming.loops.bench.BenchmarkConfig;
import org.apache.flink.streaming.util.IterationsTimer;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class StreamingPageRank implements LoopExperiment {
	
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Logger benchLogger = ExperimentSuite.getLoggerByName("LoopBench");
	/**
	 *
	 * @throws Exception
	 */
	public StreamingPageRank(BenchmarkConfig benchmarkConfig) throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism((Integer) benchmarkConfig.getParam("bench.parallelism"));
	   benchLogger.info("TEST FOO");
		DataStream<Tuple2<Long, List<Long>>> inputStream = env.addSource(new PageRankSampleSrc());
		WindowedStream<Tuple2<Long, List<Long>>, Long, TimeWindow> winStream =

			inputStream.keyBy(new KeySelector<Tuple2<Long, List<Long>>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, List<Long>> value) throws Exception {
					return value.f0;
				}
			}).timeWindow(Time.milliseconds((long) benchmarkConfig.getParam("window.size")));

		Integer numIterations = (Integer) benchmarkConfig.getParam("iteration.count");
		//TODO add proper sink
		if(numIterations > 0) {
			//STATIC SUPERSTEP VERSION
			winStream.
				iterateFixpoint(numIterations,
					new MyWindowLoopFunction(),
					new MyFeedbackBuilder(),
					new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO))
				.print();
		}
		else{
			//FIXPOINT VERSION
			winStream.
				iterateSyncDelta(
					new MyWindowLoopFunction(),
					new MyFeedbackBuilder(),
					new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO))
				.print();
		}

		env.getConfig().setExperimentConstants(
			(Integer) benchmarkConfig.getParam("window.count"), 
			(Long) benchmarkConfig.getParam("window.size"), 
			(String) benchmarkConfig.getParam("bench.id"));

		//TODO uncomment and add proper logging hooks
	
		
	}

	@Override
	public void execute() throws Exception {
		JobExecutionResult result = env.execute("Streaming Sync Iteration Example");
//		logger.info("Job duration: " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms");
	}

	private static class MyFeedbackBuilder implements FeedbackBuilder<Tuple2<Long, Double>, Long> {
		@Override
		public KeyedStream<Tuple2<Long, Double>, Long> feedback(DataStream<Tuple2<Long, Double>> input) {
			return input.keyBy(new KeySelector<Tuple2<Long, Double>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, Double> value) throws Exception {
					return value.f0;
				}
			});
		}
	}

	private static final List<Tuple3<Long, List<Long>, Long>> sampleStream = Lists.newArrayList(

		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l, 3l), 1000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(1l), 1000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(1l), 1000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(1l, 3l), 2000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l), 2000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(2l), 2000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(2l, 1l), 3000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(3l), 3000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(3l), 3000l),
		new Tuple3<>(4l, (List<Long>) Lists.newArrayList(1l), 4000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(4l), 4000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l), 4000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(1l), 4000l),
		new Tuple3<>(5l, (List<Long>) Lists.newArrayList(1l), 5000l)

	);


	private static class PageRankSampleSrc extends RichSourceFunction<Tuple2<Long, List<Long>>> {

		@Override
		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) throws Exception {
			long curTime = -1;
			for (Tuple3<Long, List<Long>, Long> next : sampleStream) {
				ctx.collectWithTimestamp(new Tuple2<>(next.f0, next.f1), next.f2);

				if (curTime == -1) {
					curTime = next.f2;
				}
				if (curTime < next.f2) {
					curTime = next.f2;
					ctx.emitWatermark(new Watermark(curTime - 1));
				}
			}
		}

		@Override
		public void cancel() {
		}
	}
	
	private static class PageRankFileSource extends RichParallelSourceFunction<Tuple2<Long, List<Long>>> {
		private int numberOfGraphs;
		private String directory;

		public PageRankFileSource(int numberOfGraphs, String directory) throws Exception {
			this.numberOfGraphs = numberOfGraphs;
			this.directory = directory;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) throws Exception {
			String path = directory + "/" + getRuntimeContext().getNumberOfParallelSubtasks() + "/part-" + getRuntimeContext().getIndexOfThisSubtask();
			for (int i = 0; i < numberOfGraphs; i++) {
				BufferedReader fileReader = new BufferedReader(new FileReader(path));
				String line;
				while ((line = fileReader.readLine()) != null) {
					String[] splitLine = line.split(" ");
					Long node = Long.parseLong(splitLine[0]);
					List<Long> neighbours = new LinkedList<>();
					for (int neighbouri = 1; neighbouri < splitLine.length; ++neighbouri) {
						neighbours.add(Long.parseLong(splitLine[neighbouri]));
					}
					ctx.collectWithTimestamp(new Tuple2<>(node, neighbours), i);
				}
				ctx.emitWatermark(new Watermark(i));
			}
		}

		@Override
		public void cancel() {
		}
	}

	private static class MyWindowLoopFunction implements WindowLoopFunction<Tuple2<Long, List<Long>>, Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>, Long, Tuple2<Set<Long>, Double>>, Serializable {

		@Override
		public void entry(LoopContext<Long, Tuple2<Set<Long>, Double>> ctx, Iterable<Tuple2<Long, List<Long>>> iterable, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, Double>>> collector) throws Exception {
			IterationsTimer entryTimer = new IterationsTimer("ET," + ctx.getKey() + "," + ctx.getContext());

			System.err.println("PRE-ENTRY:: " + ctx);

			//add all neighbors in one set (if more than one adjacency lists exist in window)

			//starting state
			Set<Long> neighborsInWindow = new HashSet<>();
			double rank = 1.0;
			for (Tuple2<Long, List<Long>> entry : iterable) {
				neighborsInWindow.addAll(entry.f1);
			}

			//merge local and persistent state if it exists
			Tuple2<Set<Long>, Double> existingState = ctx.persistentState();

			if (existingState != null) {
				neighborsInWindow.addAll(existingState.f0);
				rank = ctx.persistentState().f1;
			}
			Tuple2<Set<Long>, Double> updatedState = new Tuple2<>(neighborsInWindow, rank);
			ctx.loopState(updatedState);
			ctx.persistentState(updatedState);
			//initiate algorithm
			for (Long neighbor : neighborsInWindow) {
				collector.collect(new Either.Left(new Tuple2<>(neighbor, rank)));
			}

			entryTimer.stop();
//			logger.info(entryTimer.toString());

			System.err.println("POST-ENTRY:: " + ctx);
		}

		@Override
		public void step(LoopContext<Long, Tuple2<Set<Long>, Double>> ctx, Iterable<Tuple2<Long, Double>> iterable, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, Double>>> collector) throws Exception {
			IterationsTimer stepTimer = new IterationsTimer("ST," + ctx.getKey() + "," + ctx.getContext());

			System.err.println("PRE-STEP:: " + ctx);
			//derive rank from messages
			double newrank = 0.0;
			for (Tuple2<Long, Double> msg : iterable) {
				newrank += msg.f1;
			}
			//if vertex was not initiated do so from persistent state
			Tuple2<Set<Long>, Double> vertexState = null;
			if (ctx.hasLoopState()) {
				vertexState = ctx.loopState();
			} else {
				vertexState = ctx.persistentState();
			}
			double rank = newrank / (double) vertexState.f0.size();

			// progress if there has been change
			if (rank != vertexState.f1) {
				//update local state with new rank
				vertexState = new Tuple2<>(vertexState.f0, rank);
				ctx.loopState(vertexState);
				//send rank forward
				for (Long neighbor : vertexState.f0) {
					collector.collect(new Either.Left(new Tuple2<>(neighbor, rank)));
				}
			}

			stepTimer.stop();
//			logger.info(stepTimer.toString());

			System.err.println("POST-STEP:: " + ctx);
		}

		@Override
		public void finalize(LoopContext<Long, Tuple2<Set<Long>, Double>> ctx, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, Double>>> out) throws Exception {
			IterationsTimer finalizeTimer = new IterationsTimer("FT," + ctx.getKey() + "," + ctx.getContext());

			System.err.println("PRE-FINALIZE:: " + ctx);

			//update persistent state and output updated ranks
			if (ctx.hasLoopState()) {
				ctx.persistentState(ctx.loopState());
				out.collect(new Either.Right(new Tuple2(ctx.getKey(), ctx.loopState().f1)));
			}

			finalizeTimer.stop();
//			logger.info(finalizeTimer.toString());

			System.err.println("POST-FINALIZE:: " + ctx);
		}

		@Override
		public TypeInformation<Tuple2<Set<Long>, Double>> getStateType() {
			return TypeInformation.of(new TypeHint<Tuple2<Set<Long>, Double>>() {
			});
		}

	}
}
