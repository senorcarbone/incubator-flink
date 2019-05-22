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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * Tests for {@link WindowOperator}.
 */
@SuppressWarnings("serial")
public class MultiPassWindowOperatorTest extends TestLogger {

	private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
			TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

	private static AtomicInteger closeCalled = new AtomicInteger(0);


	@Test
	@SuppressWarnings("unchecked")
	public void testMultipassOperator() throws Exception {

		closeCalled.set(0);
		
		WindowLoopFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> loopFun = new WindowLoopFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
			@Override
			public void entry(LoopContext<String> ctx, Iterable<Tuple2<String, Integer>> input, Collector<Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> out) throws Exception {
				System.err.println("ENTRY CALLED");
			}

			@Override
			public void step(LoopContext<String> ctx, Iterable<Tuple2<String, Integer>> input, Collector<Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> out) throws Exception {
				System.err.println("STEP CALLED");
				
			}

			@Override
			public void onTermination(LoopContext<String> ctx, Collector<Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> out) throws Exception {
				System.err.println("TERMINATION CALLED");
			}
		};


		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents", STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowAssigner assigner = TumblingEventTimeWindows.of(Time.milliseconds(1));
		WindowOperator stepOperator =  new WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow>(assigner,
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new WrappedWindowFunction2<Tuple2<String, Integer>,Tuple2<String, Integer>,String, TimeWindow>(loopFun)),
				EventTimeTrigger.create(),
				0,
				null);

		WindowMultiPassOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = 
		new WindowMultiPassOperator(new TupleKeySelector(), new TupleKeySelector(),stepOperator, loopFun);

		//TEST MULTIPASS
		TwoInputStreamOperatorTestHarness testHarness = createTestHarness(operator);
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		
		testHarness.open();
		testHarness.processElement1(new StreamRecord<>(new Tuple2<>("1", 1),  3999));
		testHarness.processElement1(new StreamRecord<>(new Tuple2<>("1", 1),  3999));
		testHarness.processWatermark1(new Watermark(3999));
		
		
		
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();

		// we close once in the rest...
		Assert.assertEquals("Close was not called.", 2, closeCalled.get());
	}
	
	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	private static <OUT> TwoInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>, Either<Tuple2<String, Integer>, Tuple2<String, Integer>>> createTestHarness(
		TwoInputStreamOperator<Tuple2<String, Integer>,
			Tuple2<String, Integer>, OUT> operator) throws Exception {
		return new KeyedTwoInputStreamOperatorTestHarness(
			operator, new MultiPassWindowOperatorTest.TupleKeySelector(),
			new MultiPassWindowOperatorTest.TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO);
	}



	private static class WrappedWindowFunction2<IN, OUT, K, W extends TimeWindow> extends RichWindowFunction<IN,OUT,K,W> {

		WindowLoopFunction loopFunction;

		public WrappedWindowFunction2(WindowLoopFunction loopFunction) {
			this.loopFunction = loopFunction;
		}

		public void apply(K key, W window, Iterable<IN> input, Collector<OUT> out) throws Exception {
			loopFunction.step(new LoopContext(window.getTimeContext(), window.getEnd(), key, (StreamingRuntimeContext) getRuntimeContext()), input, out);
		}
	}
	
	private static class PassThroughFunction implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(String k, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (Tuple2<String, Integer> in: input) {
				out.collect(in);
			}
		}
	}

	private static class RichSumReducer<W extends Window> extends RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, W> {
		private static final long serialVersionUID = 1L;

		private boolean openCalled = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			closeCalled.incrementAndGet();
		}

		@Override
		public void apply(String key,
				W window,
				Iterable<Tuple2<String, Integer>> input,
				Collector<Tuple2<String, Integer>> out) throws Exception {

			if (!openCalled) {
				fail("Open was not called");
			}
			int sum = 0;

			for (Tuple2<String, Integer> t: input) {
				sum += t.f1;
			}
			out.collect(new Tuple2<>(key, sum));

		}

	}

	@SuppressWarnings("unchecked")
	private static class Tuple2ResultSortComparator implements Comparator<Object>, Serializable {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Tuple2<String, Integer>> sr0 = (StreamRecord<Tuple2<String, Integer>>) o1;
				StreamRecord<Tuple2<String, Integer>> sr1 = (StreamRecord<Tuple2<String, Integer>>) o2;
				if (sr0.getTimestamp() != sr1.getTimestamp()) {
					return (int) (sr0.getTimestamp() - sr1.getTimestamp());
				}
				int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
				if (comparison != 0) {
					return comparison;
				} else {
					return sr0.getValue().f1 - sr1.getValue().f1;
				}
			}
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
