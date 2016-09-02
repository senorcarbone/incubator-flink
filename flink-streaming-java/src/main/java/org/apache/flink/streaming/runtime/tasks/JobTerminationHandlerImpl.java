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

import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.iterative.termination.JobTerminationCoordinator;
import org.apache.flink.runtime.iterative.termination.TaskWorkingStatus;
import org.apache.flink.runtime.iterative.termination.WorkingStatusUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 *This handler is part of any non stream source task. Its main responsibility is to respond to
 *  {@link JobTerminationCoordinator} with its current working status. The following logic is implemented in this class.
 * <pre>
 *
 *         Number of Channels = C
 *
 *                         +-------+
 *         Initial State : | Idle  |   ,
 *                         |   0   |
 *                         +-------+
 *
 *                /----\     /--------------\
 *         Events:|Data| ,   |Status Request|
 *                \----/     \--------------/
 *
 *
 *                  O-----------O    O--------------O
 *         Actions :|Send Status|  , |Forward Status| ,
 *                  O-----------O    |   Request    |
 *                                   O--------------O
 *
 *                                                               .'.
 *                +-------+        /--------------\            .'   `.  No      +-------+
 *                | Idle  |------->|Status Request|--------->.' i+1=C `.------->| Idle  |
 *                |  i    |<--     \--------------/          `.       .'        |  i+1  |
 *                +---+---+  |                                 `.   .'          +---+---+
 *                    |      |                                   `.'                |
 *                    |   [set i=0]                               |                 |
 *                    |      |                                    |Yes              |
 *                    |      |   O-----------O  O--------------O  |                 |
 *                  /-+--\   +---|Send Status|--|Forward Status|<-+               /-+--\
 *                  |Data|       O-----------O  |   Request    |  |               |Data|
 *                  \-+--/                      O--------------O  |               \-+--/
 *                    |                                           |                 |
 *                    |                                           |                 |
 *                    V                                           | Yes             |
 *                +---+----+                                     .|.            +---+----+
 *            ____|Working |        /--------------\           .'   `.  No      |Working |_____
 *           |    |   i    | -------|Status Request|-------->.' i+1=C `.------->|   i+1  |    |
 *           |    +---+----+        \--------------/         `.       .'        +---+--+-+    |
 *         /++--\     ^                                        `.   .'                 |    /++--\
 *         |Data|     |                                          `.'                   |    |Data|
 *         \-+--/     |                                                                |    \-+--/
 *           |________|                                                                |______|
 *
 *
 *
 *<pre/>
 * <ul>
 *     <li>Whenever a task receives a data record, it changes its status to working.</li>
 *     <li>A task receive a {@link WorkingStatusUpdate} event from upstream channels.</li>
 *     <li>When the task receives the same event from all channels, it will do the following:
 *     <ol>
 *         <li>Respond to the {@link JobTerminationCoordinator} with
 *         a {@link TaskWorkingStatus} message containing its current status</li>
 *         <li>If the current task is not iteration head it will forward the event broadcast down the stream graph</li>
 *         <li>resets the current status to <code>Idle</code></li>
 *     </ol>
 *
 * <ul/>
 *
 */
public class JobTerminationHandlerImpl implements JobTerminationHandler {

	private static final Logger LOG = LoggerFactory.getLogger(JobTerminationHandlerImpl.class);

	StreamTask<?,?> task;

	private final int numberOfInputChannels ;

	boolean isIdle;

	private final MapWithValueCounter<Integer,Boolean> isEventReceived; // for all channels

	private WorkingStatusUpdate currentEvent =null;

	public JobTerminationHandlerImpl(StreamTask<?,?> task ){

		this.task = task;
		isIdle =  false; // safer than true
		Boolean valueToCount = Boolean.TRUE;
		isEventReceived = new MapWithValueCounter<>(valueToCount);

		InputGate[] inputGates = task.getEnvironment().getAllInputGates();
		int currentNumberOfInputChannels = 0;
		for (InputGate inputGate : inputGates) {
			for(int i=0;i<inputGate.getNumberOfInputChannels();i++){
				isEventReceived.put(currentNumberOfInputChannels+i,false);
			}
			currentNumberOfInputChannels += inputGate.getNumberOfInputChannels();
		}

		if(isEventReceived.size()==0){ // Head tasks do not have input gates, only a blocking queue
			isEventReceived.put(0,false);
		}

		this.numberOfInputChannels = isEventReceived.size();
	}

	public void onStatusEvent(WorkingStatusUpdate event, int currentChannel){

		LOG.info("Task {} received a status event {} from channel {} / {}",task.getName(),event,currentChannel,numberOfInputChannels);
		if(currentEvent!= null && event.getSequenceNumber() < currentEvent.getSequenceNumber()){
			return; // should not happen, should we throw exception?
		}

		if(currentEvent == null || event.getSequenceNumber() > currentEvent.getSequenceNumber() ){
			currentEvent = event;
			isEventReceived.resetValuesTo(false);
		}
		// At this point, currentEvent is equal to event
		if(!isEventReceived.containsKey(currentChannel)){
			String msg = task.getName() + " , Receiving a status update from unknown channel : "+currentChannel;
			throw new RuntimeException(msg);
		}
		// updating the map
		isEventReceived.put(currentChannel,true);
		if(isEventReceived.getPositiveCount()==numberOfInputChannels){
			onAllRequestsReceived();
		}

	}

	public void onStreamRecord(Object record, int currentChannel){
		// Commented, overhead
		//if(!isEventReceived.containsKey(currentChannel)){
		//	throw new RuntimeException("Unknown channel "+currentChannel);
		//}
		isIdle = false;
	}

	private void onAllRequestsReceived() {
		LOG.info("Receiving status request from all {} chanels",numberOfInputChannels);
		respondCurrentStatusToCoordinator();
		reset();
	}
	private void reset(){
		isIdle = true;
		isEventReceived.resetValuesTo(false);
	}

	//------------------------------------------------------
	private void respondCurrentStatusToCoordinator() {
		LOG.info("Responding to coordinator with idle state : {}, SEQ : ",isIdle,currentEvent.getSequenceNumber());
		// notifying
		this.task.getEnvironment().replyWorkingStatus(currentEvent.getSequenceNumber(),isIdle);
		try {
			if(!(this.task instanceof StreamIterationHead)) {
				this.task.forwardEvent(currentEvent);
			}else{
				LOG.debug("Head task {} will not forward the status update msg",task.getName());
			}
		} catch (IOException|InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	class MapWithValueCounter<K,V> extends HashMap<K,V>{
		int positiveCount=0;
		final V positiveValue;
		MapWithValueCounter(V positiveValue){
			this.positiveValue =positiveValue;
		}
		public V put(K key, V value) {
			V prev = get(key);
			if(value== positiveValue){
				if(prev != positiveValue) {
					positiveCount++;
				}
			}else{
				if(prev == positiveValue){
					positiveCount--;
				}
			}
			return super.put(key,value);
		}

		public int getPositiveCount() {
			return positiveCount;
		}
		public boolean isAllPositive(){
			return positiveCount == size();
		}
		public void resetValuesTo(V value){
			for (K key : keySet()) {
				put(key, value);
			}
		}
	}
}
