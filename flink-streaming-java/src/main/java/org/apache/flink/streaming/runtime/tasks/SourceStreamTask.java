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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.iterative.termination.AbstractLoopTerminationMessage;
import org.apache.flink.runtime.iterative.termination.BroadcastStatusUpdateEvent;
import org.apache.flink.runtime.iterative.termination.StopInputTasks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Task for executing streaming sources.
 *
 * One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the StreamFunction that it must only modify its state or emit elements in
 * a synchronized block that locks on the lock Object. Also, the modification of the state
 * and the emission of elements must happen in the same block of code that is protected by the
 * synchronized block.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 */
@Internal
public class SourceStreamTask<OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
	extends StreamTask<OUT, OP> {
	private static final Logger LOG = LoggerFactory.getLogger(SourceStreamTask.class);
	/**
	 * Source tasks are not involved in the comunication with coordinator regarding status update
	 * */
	@Override
	public LoopTerminationHandler getLoopTerminationHandler() {
		return null;
	}
	private StreamClosure streamClosure;
	@Override
	protected void init() {
		// does not hold any resources, so no initialization needed
	}

	@Override
	protected void cleanup() {
		// does not hold any resources, so no cleanup needed
	}
	

	@Override
	protected void run() throws Exception {
		headOperator.run(getCheckpointLock());
		streamClosure = new StreamClosure(this);
		LOG.info("Stream {} Completed, Starting closure!",getName());
		streamClosure.start();
	}
	
	@Override
	protected void cancelTask() throws Exception {
		headOperator.cancel();
		streamClosure.stop();
	}

	@Override
	public boolean onLoopTerminationCoordinatorMessage(AbstractLoopTerminationMessage msg) throws IOException, InterruptedException {
		boolean handled = super.onLoopTerminationCoordinatorMessage(msg);
		if(msg instanceof BroadcastStatusUpdateEvent){
			streamClosure.broadcastStatusEvent(((BroadcastStatusUpdateEvent) msg).getSequenceNumber());
			return true;
		}else if(msg instanceof StopInputTasks){
			streamClosure.stop();
		}
		return handled;
	}

}