/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link WindowOperator}.
 */
@SuppressWarnings("serial")
public class MultiPassWindowOperatorTest extends TestLogger {

	private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
		TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
		});

	private static AtomicInteger closeCalled = new AtomicInteger(0);


	@Test
	@SuppressWarnings("unchecked")
	public void testMultipassOperator() throws Exception {

		closeCalled.set(0);

		WindowLoopFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> loopFun =
			new WindowLoopFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {

				private MapState<String, Integer> count;

				private final MapStateDescriptor<String, Integer> mapStateDesc =
					new MapStateDescriptor<>("counts", StringSerializer.INSTANCE, IntSerializer.INSTANCE);

				@Override
				public void entry(LoopContext<String> ctx, Iterable<Tuple2<String, Integer>> input, Collector<Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> out) throws Exception {
					System.err.println("ENTRY CALLED for key " + ctx.getKey());
					checkAndInitState(ctx);
					for (Tuple2<String, Integer> val : input) {
						count.put(ctx.getKey(), (count.contains(ctx.getKey()) ? count.get(ctx.getKey()) : 0) + val.f1);
					}
					out.collect(Either.Left(new Tuple2<>(ctx.getKey(), count.get(ctx.getKey()))));
				}

				@Override
				public void step(LoopContext<String> ctx, Iterable<Tuple2<String, Integer>> input, Collector<Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> out) throws Exception {
					System.err.println("STEP "+ctx.getSuperstep()+" INVOKED for key "+ctx.getKey());
					count.put(ctx.getKey(),count.get(ctx.getKey()) - 1);
					Integer currentVal = count.get(ctx.getKey());
					if(currentVal > 0){
						out.collect(Either.Left(new Tuple2<>(ctx.getKey(), currentVal)));
					}
				}

				@Override
				public void onTermination(LoopContext<String> ctx, Collector<Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> out) throws Exception {
					System.err.println("TERMINATION called ");
					for (String entry : count.keys()) {
						out.collect(Either.Right(new Tuple2<>(entry, Integer.MAX_VALUE)));
					}
				}

				private void checkAndInitState(LoopContext ctx) {
					if (!mapStateDesc.isSerializerInitialized()) {
						mapStateDesc.initializeSerializerUnlessSet(ctx.getRuntimeContext().getExecutionConfig());
					}
					count = ctx.getRuntimeContext().getMapState(mapStateDesc);
				}
			};


		ExecutionConfig config = new ExecutionConfig();
		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents", STRING_INT_TUPLE.createSerializer(config));

		WindowAssigner assigner = TumblingEventTimeWindows.of(Time.milliseconds(1));
		WindowOperator stepOperator = new WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow>(assigner,
			new TimeWindow.Serializer(),
			new TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO.createSerializer(config),
			stateDesc,
			new InternalIterableWindowFunction<>(new WrappedWindowFunction2<>(loopFun)),
			EventTimeTrigger.create(),
			0,
			null);

		WindowMultiPassOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowMultiPassOperator(new TupleKeySelector(), new TupleKeySelector(), stepOperator, loopFun);

		//TEST MULTIPASS
		TwoInputStreamOperatorTestHarness testHarness = createTestHarness(operator);
		testHarness.setup(new EitherSerializer<>(STRING_INT_TUPLE.createSerializer(config), STRING_INT_TUPLE.createSerializer(config)));
		
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		ConcurrentLinkedQueue<Object> history = new ConcurrentLinkedQueue<>();
		
		testHarness.open();
		
		//Entry
		List<Long> context = new ArrayList<>();
		context.add(3999l);
		testHarness.processElement1(new StreamRecord<>(new Tuple2<>("1", 1), context, 0));
		testHarness.processElement1(new StreamRecord<>(new Tuple2<>("2", 1), context, 0));
		testHarness.processElement1(new StreamRecord<>(new Tuple2<>("2", 1), context, 0));
		testHarness.processWatermark1(new Watermark(context,0));
		System.err.println(testHarness.getOutput());
		//STEP 1
		progressStep(testHarness, history);
		System.err.println(testHarness.getOutput());
		//STEP 2
		progressStep(testHarness, history);
		System.err.println(testHarness.getOutput());
		//FINALIZE STEP
		progressStep(testHarness, history);
		//CLEANUP
		progressStep(testHarness, history);
		cleanup(testHarness.getOutput());
		testHarness.close();
		
		System.err.println(history);
	}

	private void progressStep(TwoInputStreamOperatorTestHarness testHarness,ConcurrentLinkedQueue<Object> history) throws Exception {
		history.addAll(testHarness.getOutput());

		int evtNum = testHarness.getOutput().size();
		if(evtNum == 1){
			Watermark last = new Watermark((Watermark) testHarness.getOutput().poll());
			last.setIterationDone(true);
			last.forwardTimestamp();
			testHarness.processWatermark2(last);
		}
		else{
			for(int evtCnt = 0 ; evtCnt < evtNum; evtCnt++) {
				Object o = testHarness.getOutput().poll();
				if (o instanceof Watermark) {
					Watermark next = new Watermark((Watermark) o);
					next.forwardTimestamp();
					testHarness.processWatermark2(next);
				} else {
					StreamRecord<Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> out = (StreamRecord<Either<Tuple2<String, Integer>, Tuple2<String, Integer>>>) o;
					if (out.getValue().isLeft()) {
						out.forwardTimestamp();
						testHarness.processElement2(new StreamRecord<>(out.getValue().left(), out.getProgressContext(), out.getTimestamp()));
					}
				}
			}
		}
	}

	private void cleanup(ConcurrentLinkedQueue output) {
		while (!output.isEmpty()) {
			output.poll();
		}
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	private static TwoInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>, Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> createTestHarness(
		TwoInputStreamOperator<Tuple2<String, Integer>,
			Tuple2<String, Integer>, Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> operator) throws Exception {
		return new KeyedTwoInputStreamOperatorTestHarness(
			operator, new MultiPassWindowOperatorTest.TupleKeySelector(),
			new MultiPassWindowOperatorTest.TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO);
	}


	private static class WrappedWindowFunction2<IN, OUT, K, W extends TimeWindow> extends RichWindowFunction<IN, OUT, K, W> {

		WindowLoopFunction loopFunction;

		public WrappedWindowFunction2(WindowLoopFunction loopFunction) {
			this.loopFunction = loopFunction;
		}

		public void apply(K key, W window, Iterable<IN> input, Collector<OUT> out) throws Exception {
			loopFunction.step(new LoopContext(window.getTimeContext(), window.getEnd(), key, (StreamingRuntimeContext) getRuntimeContext()), input, out);
		}
	}

	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}

}
