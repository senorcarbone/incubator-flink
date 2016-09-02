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
package org.apache.flink.runtime.iterative.termination;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A coordinator that listen to  stream sources finish state and starts the
 * process of termination detection. It ends with sending a {@link StopInputTasks} event message to
 * all input tasks to stop, which will propagate the stop to all successors

<pre>
 *                                                             ┌──────────┐          ┌──────────┐
 * ┌────────────────────┐          ┌───────────┐               │Heads and │          │Heads and │
 * │  Stream  Sources   │          │Coordinator│               │Sources   │          │non-source│
 * └─────────┬──────────┘          └─────┬─────┘               └────┬─────┘          └────┬─────┘
 *           │      StreamCompleted      │                          │                     │
 *           │ ─────────────────────────>│                          │                     │
 *           │                           │                          │                     │
 *           │                           ────┐                      │                     │
 *           │                               │ Wait for all sources │                     │
 *           │                           <───┘                      │                     │
 *           │                           │                          │                     │
 *           │                           │                          │                     │
 *           │ ╔═══════╤═════════════════╪══════════════════════════╪═════════════════════╪══════════════════════════════╗
 *           │ ║ LOOP  │  until all are done                        │                     │                              ║
 *           │ ╟───────┘                 │                          │                     │                              ║
 *           │ ║         ╔═══════╤═══════╪══════════════════════════╪═══════════════╗     │                              ║
 *           │ ║         ║ LOOP  │  for all input vertices          │               ║     │                              ║
 *           │ ║         ╟───────┘       │                          │               ║     │                              ║
 *           │ ║         ║               │BroadcastStatusUpdateEvent│               ║     │                              ║
 *           │ ║         ║               │──────────────────────────>               ║     │                              ║
 *           │ ║         ╚═══════════════╪══════════════════════════╪═══════════════╝     │                              ║
 *           │ ║                         │                          │ WorkingStatusUpdate │                              ║
 *           │ ║                         │                          │ ────────────────────>                              ║
 *           │ ║                         │                          │                     │────┐                         ║
 *           │ ║                         │                          │                     │    │ Wait for all channels   ║
 *           │ ║                         │                          │                     │<───┘                         ║
 *           │ ║                         │                          │                     │                              ║
 *           │ ║                         │                          │                     │────┐                         ║
 *           │ ║                         │                          │                     │    │ Forward "Status-update" ║
 *           │ ║                         │                          │                     │<───┘ message to sucessors    ║
 *           │ ║                         │       "TaskWorkingStatus (DONE/NOT-YET)"       │                              ║
 *           │ ║                         │<────────────────────────────────────────────────                              ║
 *           │ ║         ╔══════╤════════╪══════════════════════════╪═══════════════╗     │                              ║
 *           │ ║         ║ ALT  │  all-are-done                     │               ║     │                              ║
 *           │ ║         ╟──────┘        │                          │               ║     │                              ║
 *           │ ║         ║               │   StopInputTasks,end     │               ║     │                              ║
 *           │ ║         ║               │──────────────────────────>               ║     │                              ║
 *           │ ║         ╚═══════════════╪══════════════════════════╪═══════════════╝     │                              ║
 *           │ ╚═════════════════════════╪══════════════════════════╪═════════════════════╪══════════════════════════════╝
 * ┌─────────┴──────────┐          ┌─────┴─────┐               ┌────┴─────┐          ┌────┴─────┐
 * │  Stream  Sources   │          │Coordinator│               │Heads and │          │Heads and │
 * └────────────────────┘          └───────────┘               │Sources   │          │non-source│
 *                                                             └──────────┘          └──────────┘                                                                        └──────────┘           └──────────┘
 </pre>

 */
public class LoopTerminationCoordinator{
	protected static final Logger LOG = LoggerFactory.getLogger(LoopTerminationCoordinator.class);
	private final Map<JobVertexID,ExecutionJobVertex> sourceVertices;
	private final List<ExecutionJobVertex> headVertices;
	private final JobID jobId;
	private final int numOfSources;
	private final Set<JobVertexID> finishedSources = new HashSet<>();
	private boolean terminated = false;
	private final Map<JobVertexID, ExecutionJobVertex> allVertices;
	private final Map<JobVertexID, ExecutionJobVertex> nonInputVertecies;
	private final Map<ExecutionAttemptID, Boolean> respondingSubtasksStatus;
	private long sequenceNumber  = 0;
	private final int expectedNumberOfResponses;
	private boolean previousStepDone=false;
	private final boolean loopsExist;

	public LoopTerminationCoordinator(JobID _jobId, Map<JobVertexID, ExecutionJobVertex> _allVertices){
		this.allVertices = _allVertices;
		this.sourceVertices =  new HashMap<>();
		this.nonInputVertecies = new HashMap<>();

		this.headVertices = new ArrayList<>();
		for(Map.Entry<JobVertexID, ExecutionJobVertex> e : allVertices.entrySet()){
			if(e.getValue().getJobVertex().isSourceInputVertex()){
				sourceVertices.put(e.getKey(),e.getValue());
			}else{
				if(e.getValue().getJobVertex().isInputVertex()){ // and not a source input vertex
					headVertices.add(e.getValue());
				}else{
					nonInputVertecies.put(e.getKey(),e.getValue());
				}
			}
		}
		int numOfRespondingSubTask = 0;
		for(ExecutionJobVertex task : nonInputVertecies.values()){
			numOfRespondingSubTask+=task.getTaskVertices().length;
		}

		for(ExecutionJobVertex task :headVertices){
			numOfRespondingSubTask+=task.getTaskVertices().length;
		}

		this.expectedNumberOfResponses = numOfRespondingSubTask;
		respondingSubtasksStatus = new HashMap<>();
		loopsExist = headVertices.size()>0;
		this.jobId = _jobId;
		this.numOfSources = sourceVertices.size();
	}

	public void onStreamCompleted(StreamCompleted event){

		LOG.info("Coordinator ",event);
		if(finishedSources.size() == this.numOfSources){
			return ;// should throw exception
		}
		if(terminated){
			return;
		}

		JobID eventJobId = event.getJobID();
		JobVertexID  taskVertexID = event.getTaskVertexID();

		checkArgument(this.jobId.equals(eventJobId),
				"Job ids ; current: {} and provided : {} does not match"
				,this.jobId,eventJobId);

		if(sourceVertices.containsKey(taskVertexID)) { // we only listen to source vertexes
			checkArgument(!terminated,"Receiving source finish event {} after termination!",taskVertexID);
			checkArgument(!finishedSources.contains(taskVertexID),
					"Receiving an event twice for the same source vertex {}", taskVertexID);

			finishedSources.add(taskVertexID);

			if(finishedSources.size() == this.numOfSources){
				if(!loopsExist){
					terminate();
				}else {
					askForStatus();
				}
			}
		}

	}
	private void askForStatus(){
		sequenceNumber++;
		triggerUpdateEventToInputVertecies();
	}
	public void onTaskStatus(TaskWorkingStatus taskStatus){
		if(taskStatus.getSequenceNumber() != sequenceNumber){
			return; // it is not right
		}

		if(terminated){
			return;
		}

		checkArgument(this.jobId.equals(taskStatus.getJobID()),
				"Job ids ; current: {} and provided : {} does not match"
				,this.jobId,taskStatus.getJobID());

		ExecutionJobVertex ejv = allVertices.get(taskStatus.getTaskVertexID());
		if(nonInputVertecies.containsValue(ejv) || headVertices.contains(ejv)){
			respondingSubtasksStatus.put(taskStatus.getTaskExecutionId(),taskStatus.isWaiting());
			LOG.debug("Total received responses at round {} are : {}/{}",
					sequenceNumber,respondingSubtasksStatus.size(),expectedNumberOfResponses);
			if(respondingSubtasksStatus.size() == expectedNumberOfResponses){
				onRoundClosure();
			}
		}else{
			System.out.println("error : recieving data from unknown vertext "+taskStatus.getTaskVertexID());
		}
	}

	private void onRoundClosure() {
		boolean allTasksDone = getAllStatus();
		if(allTasksDone){
			if(previousStepDone) {
				terminate();
				return;
			}
		}
		previousStepDone = allTasksDone;
		reset();
		askForStatus();

	}

	private boolean getAllStatus(){
		int numTrue=0;
		for(boolean b : respondingSubtasksStatus.values()){
			if(b){
				numTrue++;
			}
		}
		boolean allStatus = (numTrue == respondingSubtasksStatus.size());
		LOG.info( "Round {} is done, all done : {} , ratio {}/{}",
				sequenceNumber,allStatus,numTrue,expectedNumberOfResponses);
		return allStatus;
	}

	private void reset(){
		respondingSubtasksStatus.clear();
	}

	private void terminate(){
		LOG.info("Sending a termination message to input vertices");
		sendStopEventTo(headVertices);
		sendStopEventTo(sourceVertices.values());
		terminated = true;
	}
	private void triggerUpdateEventToInputVertecies(){
		sendUpdateEventTo(sourceVertices.values());
		sendUpdateEventTo(headVertices);
	}
//---------------- UTILS for sending messages-----------
	private void sendUpdateEventTo(Collection<ExecutionJobVertex> list){
		for(ExecutionJobVertex task : list) {
			for(ExecutionVertex subtask : task.getTaskVertices()) {
				BroadcastStatusUpdateEvent e = new BroadcastStatusUpdateEvent(sequenceNumber,
						this.jobId,
						task.getJobVertexId(),
						subtask.getCurrentExecutionAttempt().getAttemptId());
				sendToSubTask(subtask, e);

			}
		}
	}

	private void sendStopEventTo(Collection<ExecutionJobVertex> list){
		for(ExecutionJobVertex jv : list){
			if (jv != null) {
				for (ExecutionVertex v : jv.getTaskVertices()) {
					ExecutionAttemptID attemptID= v.getCurrentExecutionAttempt().getAttemptId();
					StopInputTasks msg = new StopInputTasks(this.jobId,jv.getJobVertexId(),attemptID);
					sendToSubTask(v, msg);
				}
			}
		}
	}

	private void sendToSubTask(ExecutionVertex subtask, AbstractLoopTerminationMessage msg){
		Execution execution = subtask.getCurrentExecutionAttempt();
		if (execution.getState() == ExecutionState.RUNNING) {
			ExecutionAttemptID id = execution.getAttemptId();
			subtask.sendMessageToCurrentExecution(msg, id);
		} else {
			LOG.warn("Vertex {} is done before get notified about ending stream");
		}
	}

}