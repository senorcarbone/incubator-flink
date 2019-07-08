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


package org.apache.flink.streaming.examples.iteration;

import com.google.common.collect.Lists;
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
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public class StreamingConnectedComponents {
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public static void main(String[] args) throws Exception {
		StreamingConnectedComponents example = new StreamingConnectedComponents();
		example.run();
	}


	/**
	 * @throws Exception
	 */
	public StreamingConnectedComponents() throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(4);

		DataStream<Tuple2<Long, List<Long>>> inputStream = env.addSource(new CCSampleSrc());
		WindowedStream<Tuple2<Long, List<Long>>, Long, TimeWindow> winStream =

			inputStream.keyBy(new KeySelector<Tuple2<Long, List<Long>>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, List<Long>> value) throws Exception {
					return value.f0;


				}
			}).timeWindow(Time.milliseconds(1000));

		winStream.iterateSyncDelta(
			new MyWindowLoopFunction(),
			new MyFeedbackBuilder(),
			new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
			.print();
	}

	protected void run() throws Exception {
		System.err.println(env.getExecutionPlan());
		env.execute("Streaming Sync Iteration Example (CC)");
	}

	private static class MyFeedbackBuilder implements FeedbackBuilder<Tuple2<Long, Long>, Long> {
		@Override
		public KeyedStream<Tuple2<Long, Long>, Long> feedback(DataStream<Tuple2<Long, Long>> input) {
			return input.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, Long> value) throws Exception {
					return value.f0;
				}
			});
		}
	}

	private static final List<Tuple3<Long, List<Long>, Long>> sampleStream = Lists.newArrayList(

		// vertexId - List of neighbors - timestamp
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l, 3l), 1000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(1l), 1000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(1l), 1000l),
		new Tuple3<>(4l, (List<Long>) Lists.newArrayList(5l, 6l), 1000l),
		new Tuple3<>(5l, (List<Long>) Lists.newArrayList(4l), 1000l),
		new Tuple3<>(6l, (List<Long>) Lists.newArrayList(4l), 1000l),

		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(1l, 3l), 2000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l), 2000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(2l), 2000l),
		new Tuple3<>(4l, (List<Long>) Lists.newArrayList(5l, 6l), 2000l),
		new Tuple3<>(5l, (List<Long>) Lists.newArrayList(4l), 2000l),
		new Tuple3<>(6l, (List<Long>) Lists.newArrayList(4l), 2000l),
		new Tuple3<>(10l, (List<Long>) Lists.newArrayList(8l, 9l), 2000l),
		new Tuple3<>(9l, (List<Long>) Lists.newArrayList(10l), 2000l),
		new Tuple3<>(8l, (List<Long>) Lists.newArrayList(10l), 2000l),

		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(2l, 1l), 3000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(3l), 3000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(3l), 3000l),

		new Tuple3<>(4l, (List<Long>) Lists.newArrayList(1l), 4000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(4l), 4000l)

	);


	private static class CCSampleSrc extends RichSourceFunction<Tuple2<Long, List<Long>>> {

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


	private static class MyWindowLoopFunction implements WindowLoopFunction<Tuple2<Long, List<Long>>, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>, Long, Tuple2<Set<Long>, Long>>, Serializable {

		@Override
		public void entry(LoopContext<Long, Tuple2<Set<Long>, Long>> ctx, Iterable<Tuple2<Long, List<Long>>> input, Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {
			System.err.println("PRE-ENTRY:: " + ctx);

			//starting state
			Set<Long> neighborsInWindow = new HashSet<>();
			long component = ctx.getKey();
			for (Tuple2<Long, List<Long>> entry : input) {
				neighborsInWindow.addAll(entry.f1);
			}

			//merge local and persistent state if it exists
			Tuple2<Set<Long>, Long> existingState = ctx.persistentState();

			if (existingState != null) {
				neighborsInWindow.addAll(existingState.f0);
				component = ctx.persistentState().f1;
			}
			Tuple2<Set<Long>, Long> updatedState = new Tuple2<>(neighborsInWindow, component);
			ctx.loopState(updatedState);
			ctx.persistentState(updatedState);
			//initiate algorithm
			for (Long neighbor : neighborsInWindow) {
				out.collect(new Either.Left(new Tuple2<>(neighbor, component)));
			}
			System.err.println("POST-ENTRY:: " + ctx);
		}

		@Override
		public void step(LoopContext<Long, Tuple2<Set<Long>, Long>> ctx, Iterable<Tuple2<Long, Long>> input, Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {
			System.err.println("PRE-STEP:: " + ctx);

			//derive component id from messages
			long componentID = ctx.getKey();
			for (Tuple2<Long, Long> msg : input) {
				if(msg.f1 < componentID){
					componentID = msg.f1;
				}
			}
			
			//retrieve stored state
			Tuple2<Set<Long>, Long> vertexState = null;
			if (ctx.hasLoopState()) {
				vertexState = ctx.loopState();
			} else {
				vertexState = ctx.persistentState();
			}

			// expand if component ID needs to update
			if (componentID < vertexState.f1) {
				//update local state with new component ID
				vertexState = new Tuple2<>(vertexState.f0, componentID);
				ctx.loopState(vertexState);
				//send updated component ID
				for (Long neighbor : vertexState.f0) {
					out.collect(new Either.Left(new Tuple2<>(neighbor, componentID)));
				}
			}

			System.err.println("POST-STEP:: " + ctx);
		}


		@Override
		public void finalize(LoopContext<Long, Tuple2<Set<Long>, Long>> ctx, Collector<Either<Tuple2<Long, Long>, Tuple2<Long, Long>>> out) throws Exception {
			System.err.println("PRE-FINALIZE:: " + ctx);

			//back up to persistent state and flush updated component IDs
			if (ctx.hasLoopState()) {
				ctx.persistentState(ctx.loopState());
				out.collect(new Either.Right(new Tuple2(ctx.getKey(), ctx.loopState().f1)));
			}
			System.err.println("POST-FINALIZE:: " + ctx);
		}
		
		
		@Override
		public TypeInformation<Tuple2<Set<Long>, Long>> getStateType() {
			return TypeInformation.of(new TypeHint<Tuple2<Set<Long>, Long>>() {
			});
		}
	}
}
