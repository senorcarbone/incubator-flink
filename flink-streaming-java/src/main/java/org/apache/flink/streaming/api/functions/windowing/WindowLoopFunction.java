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


package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Contains User Defined Functions for a bulk synchronous iteration on WindowedStreams on input and feedback:
 * - entry: takes input windows, may save initial local state for the iteration, usually puts out to the feedback
 * - step: takes feedback windows, may update local state, usually puts out to feedback (or to output for early results)
 * - finalize: is triggered when an iteration terminates. Reads local state and puts it out to output stream.
 *
 *  Usual lifecycle of an iteration:
 *  - window from input is processed by entry function
 *  - output of entry goes to feedback (will be passed through feedbackBuilder and then windowed for BSP)
 *  - feedback window gets processed by step function
 *  - ... (more iterations: step -> feedbackBuilder -> Windowing for BSP)
 *  - StreamIterationTermination decides that the iteration is over -> finalize gets called
 *  - on Termination reads local state and puts out final results
 *
 * @param <IN>	  The input data type (goes into entry function)
 * @param <F_IN>  The feedback input data type (goes into step function)
 * @param <OUT>   The type of the iteration output (likely produced by finalize, but also entry&step can output)
 * @param <R> 	  The type of the feedback output (as produced by entry or step)
 * @param <KEY>   The key type of both input and feedback windows (should be keyed the same way anyways)
 * @param <S>     The type of state used in the iteration

 */
@Public
public interface WindowLoopFunction<IN,F_IN,OUT,R,KEY, S> extends Function, Serializable {
	void entry(LoopContext<KEY, S> ctx, Iterable<IN> input, Collector<Either<R,OUT>> out) throws Exception;
	void step(LoopContext<KEY, S> ctx, Iterable<F_IN> input, Collector<Either<R,OUT>> out) throws Exception;
	void finalize(LoopContext<KEY, S> ctx, Collector<Either<R,OUT>> out) throws Exception;
	TypeInformation<S> getStateType();

}