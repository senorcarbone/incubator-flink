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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.test.streaming.runtime.util.NoOpInt2Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@SuppressWarnings({ "unchecked", "unused", "serial" })
public class LoopTerminationITCase extends StreamingMultipleProgramsTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(LoopTerminationITCase.class);

	int streamSamples = 100;
	int BOUND = 5;
	long sourceDelay = 200L;
	int interationTimeout = 100;
	IntRepeaterStreamSource intRepeaterStreamSource = new IntRepeaterStreamSource(streamSamples,sourceDelay);
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	DataStream<Tuple2<Integer,Integer>> inputStream;
	@Before
	public void beforeTest(){
		CollectSink.reset();
		inputStream = env.addSource(intRepeaterStreamSource );
	}
	@Test
	public void singleIteration(){
		 doForIteration(inputStream, 1);
	}
	@Test
	public void doubleNonNestedIteration(){
		// IF this test is done in the old implementation it will fail
		DataStream<Tuple2<Integer,Integer>> iteration1Out = getStepForIteration(inputStream,1);
		DataStream<Tuple2<Integer,Integer>> iteration2Out = getStepForIteration(iteration1Out,-1);

		CollectSink collectIteration1 = new CollectSink("c"+1);
		iteration1Out.addSink(collectIteration1);

		CollectSink collectIteration2 = new CollectSink("c"+2);
		iteration2Out.addSink(collectIteration2);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

		List<Tuple2<Integer,Integer>> colectedIteration1 = collectIteration1.getCollected();
		//  all records are terminated after gracefuly finished
		Assert.assertEquals(colectedIteration1.size(),streamSamples);
		// each record finishes its iterations
		for(int i =0;i< colectedIteration1.size();i++){
			Tuple2<Integer,Integer> v = colectedIteration1.get(i);
			Assert.assertEquals(v.f1.intValue(),(BOUND+1)*1);
		}


		List<Tuple2<Integer,Integer>> colectedIteration2 = collectIteration2.getCollected();
		//  all records are terminated after gracefuly finished
		Assert.assertEquals(colectedIteration2.size(),streamSamples);
		// each record finishes its iterations
		for(int i =0;i< colectedIteration2.size();i++){
			Tuple2<Integer,Integer> v = colectedIteration2.get(i);
			Assert.assertEquals(v.f1.intValue(),-1);
		}


	}

	@Test
	public void nestedIteration(){
		// IF this test is done in the old implementation it will fail
		doForIterationNested(1);
	}
	@Test(expected = UnsupportedOperationException.class)
	public void overlappingIterations(){
		doForOverlappingIteration();
	}

	//=====================
	private DataStream<Tuple2<Integer,Integer>> getStepForIteration(DataStream<Tuple2<Integer,Integer>>  input ,int factor){
		IterativeStream<Tuple2<Integer,Integer>> it = input.map(new NoOpInt2Map()).iterate(interationTimeout);

		SplitStream<Tuple2<Integer,Integer>> step = it.map(new IncMapFunction(factor)).split(new BoundSelector(0,BOUND*Math.abs(factor)));

		it.closeWith(step.select("feedback"));

		return step.select("output");
	}
	private void doForIteration(DataStream<Tuple2<Integer,Integer>>  input ,int factor){

		DataStream<Tuple2<Integer,Integer>> output = getStepForIteration(input,factor);

		CollectSink collect = new CollectSink("c"+factor);
		output.addSink(collect);


		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		List<Tuple2<Integer,Integer>> colected = collect.getCollected();
		//  all records are terminated after gracefuly finished
		Assert.assertEquals(colected.size(),streamSamples);
		// each record finishes its iterations
		for(int i =0;i< colected.size();i++){
			Tuple2<Integer,Integer> v = colected.get(i);
			Assert.assertEquals(v.f1.intValue(),(BOUND+1)*factor);
		}
	}


	private void doForIterationNested(int factor){

		IterativeStream<Tuple2<Integer,Integer>> outerloop = inputStream.map(new NoOpInt2Map()).iterate(interationTimeout);
		IterativeStream<Tuple2<Integer,Integer>> interloop = outerloop.iterate(interationTimeout);

		SplitStream<Tuple2<Integer,Integer>> stepInner = interloop.map(new IncMapFunction(factor)).split(new BoundSelector(0,BOUND*factor));
		interloop.closeWith(stepInner.select("feedback"));
		CollectSink collectInternalLoop = new CollectSink("ic"+factor);
		DataStream<Tuple2<Integer,Integer>> outputInner = stepInner.select("output");
		outputInner.addSink(collectInternalLoop);

		//IterativeStream<Tuple2<Integer,Integer>> it2 = outputInner.iterate(interationTimeout);
		SplitStream<Tuple2<Integer,Integer>> stepOuter =  outputInner.map(new IncMapFunction(factor)).split(new BoundSelector(BOUND+2*factor,(2*BOUND+1)*factor));
		outerloop.closeWith(stepOuter.select("feedback"));

		CollectSink collectOuterLoop = new CollectSink("oc"+factor);
		DataStream<Tuple2<Integer,Integer>> outputOuter = stepOuter.select("output");
		outputOuter.addSink(collectOuterLoop);
		//outputOuter.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

		List<Tuple2<Integer,Integer>> colectedInner = collectInternalLoop.getCollected();
		Assert.assertEquals(streamSamples*4,colectedInner.size());
		for(int i =0;i< colectedInner.size();i++){
			int v = colectedInner.get(i).f1;
			Assert.assertTrue(v>=(BOUND+1)*factor);
		}

		List<Tuple2<Integer,Integer>> colectedOuter = collectOuterLoop.getCollected();
		Assert.assertEquals(colectedOuter.size(),streamSamples);
		for(int i =0;i< colectedOuter.size();i++){
			int v = colectedOuter.get(i).f1;
			Assert.assertEquals(v,(BOUND+1)*factor*2+1);
		}

	}


	private void doForOverlappingIteration(){
		int factor = 1;
		IterativeStream<Tuple2<Integer,Integer>> outerloop = inputStream.map(new NoOpInt2Map()).iterate(interationTimeout);
		IterativeStream<Tuple2<Integer,Integer>> interloop = outerloop.iterate(interationTimeout);

		SplitStream<Tuple2<Integer,Integer>> stepInner = interloop.map(new IncMapFunction(factor)).split(new BoundSelector(0,BOUND*factor));
		// closing the outer before the inner
		outerloop.closeWith(stepInner.select("feedback"));

	}

	 static class CollectSink implements SinkFunction<Tuple2<Integer,Integer>> {

		private static final long serialVersionUID = 1L;
		private static ConcurrentHashMap<String,CopyOnWriteArrayList<Tuple2<Integer,Integer>>> collected = new ConcurrentHashMap<>();
		 final String collectionName;
		 CollectSink(String collectionName){
		 	this.collectionName = collectionName;
			 collected.put(this.collectionName, new CopyOnWriteArrayList<Tuple2<Integer,Integer>>());
		 }

		 public List<Tuple2<Integer,Integer>> getCollected(){
		 	return collected.get(collectionName);
		 }
		@Override
		public void invoke(Tuple2<Integer,Integer> value) throws Exception {
			collected.get(collectionName).add(value);
		}

		 public static void reset() {
			 collected.clear();
		 }
	 }

	private static class IntRepeaterStreamSource implements SourceFunction<Tuple2<Integer,Integer>> {
		private static final long serialVersionUID = 1L;
		private final  int MAX_STREAMED_SAMPLES;
		private volatile boolean isRunning;
		private int counter = 0;
		private final long delayMS;
		IntRepeaterStreamSource(int maxstreamSamples,long delayMS){
			this.MAX_STREAMED_SAMPLES = maxstreamSamples;
			this.delayMS=delayMS;
		}
		@Override
		public void run(SourceContext<Tuple2<Integer,Integer>> ctx) throws Exception {
			counter = 0;
			isRunning = true;
			while (isRunning && counter < MAX_STREAMED_SAMPLES) {
				ctx.collect(new Tuple2<Integer, Integer>(counter,0));
				counter++;
				Thread.sleep(this.delayMS);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class IncMapFunction implements MapFunction<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>{
		private final int delta;
		IncMapFunction(int delta){
			this.delta = delta;
		}
		@Override
		public Tuple2<Integer,Integer> map(Tuple2<Integer,Integer> value) throws Exception {
			return new Tuple2<>(value.f0,value.f1+delta);
		}
	}

	private static class BoundSelector implements OutputSelector<Tuple2<Integer,Integer>> {
		private static final long serialVersionUID = 1L;
		// to include in the loop feedback
		private final int lowerBound;
		private final int upperBound;
		BoundSelector(int lowerBound, int upperBound){
			this.lowerBound=lowerBound;
			this.upperBound=upperBound;
		}
		@Override
		public Iterable<String> select( Tuple2<Integer,Integer> value) {
			if (value.f1 >=lowerBound && value.f1<=upperBound ) {
				return Collections.singleton("feedback");
			} else {
				return Collections.singleton("output");
			}
		}
	}

}