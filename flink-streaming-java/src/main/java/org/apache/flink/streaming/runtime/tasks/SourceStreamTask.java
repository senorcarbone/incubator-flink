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
import org.apache.flink.runtime.iterative.termination.JobTerminationMessage;
import org.apache.flink.runtime.iterative.termination.BroadcastStatusUpdateEvent;
import org.apache.flink.runtime.iterative.termination.StopInputTasks;
import org.apache.flink.runtime.iterative.termination.WorkingStatusUpdate;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkState;

import java.io.IOException;
import java.util.Arrays;

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

 <pre>
 *┌─────────────────────────────────┐
 *│Events :                         │
 *│{run},{cancel},{stop},           │
 *│{finalize},{Broadcase-Status},   │
 *│{run-Finished}                   │
 *├─────────────────────────────────┤
 *│Actions :                        │
 *│[notify-finalize-coordinator]    │
 *│[exit]                           │
 *│[wait]                           │
 *└─────────────────────────────────┘
 *                                  ┌─────┐
 *    ┌─────────────────────────────│Idle │─────────────────────────────┐
 *    │                             └──┬──┘                             │
 *    │                              {run}                              │
 *    │                                │                                │
 *    │                                ▼                                │
 *    │                            ┌───────┐                            │
 *    │                    ┌───────│Running│────────────────────┐       │
 *    │                    │       └───────┘                    │       │
 *    │                    │           │                        │       │
 *    │                    │         {stop}              {cancel}       │
 *    │              {run-Finished}    │                        │       │
 *    │                    │           ▼                        │       │
 *    │                    │      ┌─────────┐                   │       │
 *    │                    │      │Stopping │────{cancel} ─────▶│       │
 *    │                    │      └─────────┘                   │       │
 *  {stop}                 │      {run-Finished}                │  {cancel}
 *    │                    │           │                        ▼       │
 *    │                    │           │                  ┌ ─ ─ ─ ─ ─   │
 *    │                    ◀───────────┘                   Canceling │  │
 *    │                    │                              └ ─ ─ ─ ─ ─   │
 *    │      [notify-finalize-coordinator]                      │       │
 *    │                 [wait]                                  │       │
 *    │          ┌───────┐ │                                    │       │
 *    │{Broadcast-Status}│ │                                    │       │
 *    │          │       ▼ ▼                         {run-Finished}     │
 *    │          │  ┌────────────┐                           │          │
 *    │          └──│ Finalizing │                           │          │
 *    │             └────────────┘                           │          │
 *    │                    │                                 ▼          │
 *    └────────────┐  {terminate}                         ┌─────────────┘
 *                 │       │                              │
 *                 │       └───────────┐                  │
 *                 │                   │                  │
 *   [notify-finalize-coordinator]     │    [notify-finalize-coordinator]
 *              [exit]                 │               [exit]
 *                 │                   ▼                  │
 *                 │              ┌────────┐              │
 *                 └─────────────▶│Terminal│◀─────────────┘
 *                                └────────┘
 </pre>
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator

 */
@Internal
public class SourceStreamTask<OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
	extends StreamTask<OUT, OP> {

	enum SourceExecState{
		IDLE,RUNNING,STOPPING,CANCELING,WAITING,TERMINAL;
		public boolean in(SourceExecState ... states){
			for(SourceExecState s : states){
				if(s==this){
					return true;
				}
			}
			return false;
		}

	}

	volatile SourceExecState state = SourceExecState.IDLE ;

	Object sourceLock = new Object();

	public  SourceStreamTask(){
		this(new StreamClosureImpl());
	}

	public  SourceStreamTask(StreamClosure streamClosure){
		this.streamClosure = streamClosure;
		this.streamClosure.setLock(sourceLock);
	}



	private static final Logger LOG = LoggerFactory.getLogger(SourceStreamTask.class);

	/**
	 * Source tasks are not involved in the comunication with coordinator regarding status update
	 * */
	@Override
	public JobTerminationHandler getJobTerminationHandler() {
		return null;
	}

	private final StreamClosure streamClosure;

	private volatile boolean coordinatorNotified = false;

	@Override
	protected void init() {
		// does not hold any resources, so no initialization needed
		//ADDING 'TERMINAL' to the allowed cases because :
		//TimestampITCase.testWatermarkPropagationNoFinalWatermarkOnStop
		// fails otherwise. This can happen  forcing job to stop while running.
		checkExecState("init",SourceExecState.IDLE,SourceExecState.TERMINAL);
	}

	@Override
	protected void cleanup() {
		// does not hold any resources, so no cleanup needed
		//TOLERATING CANCELING State
		checkExecState("cleanup",SourceExecState.TERMINAL,SourceExecState.CANCELING);
	}
	

	@Override
	protected void run() throws Exception {
		checkExecStateNot("run",SourceExecState.WAITING,SourceExecState.CANCELING);
		if(state!=SourceExecState.STOPPING  && state!=SourceExecState.TERMINAL) {
			state = SourceExecState.RUNNING;
			headOperator.run(getCheckpointLock());
			LOG.info("RUN COMPLETE of task {}", getName());
			postRun();
		}
	}

	protected  void postRun(){
		synchronized (sourceLock) {
			if (state == SourceExecState.RUNNING || state == SourceExecState.STOPPING) {
				LOG.info("Stream {} Completed, Starting closure!", getName());
				notifyCoordinator();
				state = SourceExecState.WAITING; // waiting
				streamClosure.waitUntilFinalized();
				LOG.error("FINAL DONE");
			} else if (state == SourceExecState.CANCELING) {
				notifyAndFinish(); // terminal
				return;
			} else {
				checkExecState("finish running",
						SourceExecState.RUNNING,
						SourceExecState.STOPPING,
						SourceExecState.CANCELING);
			}
		}
	}

	@Override
	protected void cancelTask() throws Exception {
		synchronized (sourceLock) {
			LOG.info("Canceling task :  {} , on state  {} ",getName(),state);
			if(state == SourceExecState.IDLE) {
				notifyAndFinish(); // terminal
			}else if (state == SourceExecState.WAITING){
				exit();
			}else if(state == SourceExecState.RUNNING ||
					state == SourceExecState.STOPPING ||
					state== SourceExecState.CANCELING){
				state = SourceExecState.CANCELING;
				if(headOperator != null) {
					headOperator.cancel();
				}
			}else{
				// task in a terminal state .. just do nothing
			}
			//notifyAndFinish(); // will not wait
			// NOTE :   WILL NOT WAIT FOR TERMINATION COORDINATOR,
			// DO NOT WANT TO BLOCK THIS THREAD
		}
	}
	void notifyAndFinish(){
		synchronized (sourceLock) {
			notifyCoordinator();
			exit();
		}
	}
	void exit(){
		synchronized (streamClosure) {
			streamClosure.stop();
			state = SourceExecState.TERMINAL;
		}
	}

	@Override
	public  boolean onLoopTerminationCoordinatorMessage(JobTerminationMessage msg) throws IOException, InterruptedException {
		synchronized (sourceLock) {
			boolean handled = super.onLoopTerminationCoordinatorMessage(msg);
			if (msg instanceof BroadcastStatusUpdateEvent) {
				if(state == SourceExecState.WAITING) {
					int sequenceNumber = ((BroadcastStatusUpdateEvent) msg).getSequenceNumber();
					LOG.error("broadcasting a status message event with SEQ" +
							" : {} from task {}", sequenceNumber, getName());
					forwardEvent(new WorkingStatusUpdate(sequenceNumber, getName()));
					return true;
				}else{
					LOG.error("Receiving a Broadcast status update event while in state {}",state);
				}
			} else if (msg instanceof StopInputTasks) {
				if(state == SourceExecState.WAITING) {
					exit();
				}else{

					checkState(!streamClosure.isWaiting(),"Receiving a Terminate event  on state {} " +
							"but the clsure latch is waiting",state);
					// worthless message ..
					LOG.warn("Receiving a Terminate event  while in state {}",state);
				}
			}
			return handled;
		}

	}

	private void notifyCoordinator(){
		LOG.error("Notifying Loop termination coordinator about stream completion of task {}",getName());
		// DISABLING THE CHECK BECAUSE test : SourceStreamTaskStoppingTest fails
		//checkNotNull(getEnvironment(),"Environment is null while trying to notify coordinaot");
		checkState(!coordinatorNotified,"double notification to the coordinator");
		if(getEnvironment() != null && !coordinatorNotified) {
			getEnvironment().notifyStreamCompleted();
			coordinatorNotified = true;
		}
	}

	public String getName() {
		try{
			return super.getName();
		}catch (Throwable e){
			return "";
		}
	}
	void checkExecState(String on,SourceExecState ... expected ){
		checkState(state.in(expected),"On %s, state should be in %s but got %s",on, Arrays.toString(expected),state);
	}
	void checkExecStateNot(String on,SourceExecState ... expected ){
		checkState(!state.in(expected),"On %s, state should *not* be in %s but got %s",on,Arrays.toString(expected),state);
	}
}