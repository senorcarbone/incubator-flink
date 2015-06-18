package org.apache.flink.streaming.sampling.dummy;/*
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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by marthavk on 2015-06-12.
 */
public class MyWordCount {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text = env.fromElements(WordCountData.WORDS);
		text.flatMap(new WordMapper()) // Splits the text by space characters into words and maps each word to an increment of 1
			.groupBy(0) //Groups stream by word. Tuples containing the same word will be sent to the same processing unit
			.reduce(new WordReducer()) //Counts the words
			.print();
		System.err.println(env.getExecutionPlan());
		env.execute("Word Counter");

	}


}

//map
class WordMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {

	@Override
	public void flatMap(String value,
						Collector<Tuple2<String,Integer>> out)  {
		String[] words = value.split(" ");
		for (String word : words) {
			out.collect(new Tuple2<String, Integer>(word,1));
		}
	}
}

class WordReducer implements ReduceFunction<Tuple2<String, Integer>> {

	@Override
	public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
		return new Tuple2<String, Integer>(value1.f0, value1.f1+value2.f1);
	}
}
