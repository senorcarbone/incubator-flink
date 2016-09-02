/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A coordinator that listens to  stream sources completion  and starts the
 * process of termination detection. It ends with sending a {@link StopInputTasks} event message to
 * all input tasks (sources and iteration heads) to stop, this will propagate the termination to all execution graph.
 * The whole process is summarized in the following steps :
 * <ol>
 *	 <li>The coordinator receives a {@link StreamCompleted} message from  {@code StreamSource} vertices </li>
 *	 <li>After receiving the completion message from <b> all </b> {@code StreamSource} vertices, it will send a
 *	 {@link BroadcastStatusUpdateEvent} message to
 *	 all {@code StreamSource} and {@code IterationHead} vertices.</li>
 *	 <li>The receiving vertices will  forward a status update event down the stream graph to all running sub-tasks</li>
 *	 <li>Each sub-task will wait until it gets the status event from all channels, event alignments. After
 *	 that, it will forward the same event to the successor vertices and replies its status to the coordinator
 *	 via a {@link TaskWorkingStatus} message</li>
 *	 <li>The coordinator then starts receiving a {@link TaskWorkingStatus} message from all non-source sub-tasks</li>
 *	 <li>If all sub-tasks status are idle, then the coordinator will send a {@link StopInputTasks} messages
 *	 to all sources and iteration heads to trigger termination. Otherwise, the coordinator will send a
 *	 {@link BroadcastStatusUpdateEvent} message again and repeat the same logic </li>
 * </ol>

<pre>

 *                                                                ┌──────────┐          ┌──────────┐
 *     ┌───────────────────┐          ┌───────────┐               │Heads and │          │Heads and │
 *     │Stream Sources     │          │Coordinator│               │Sources   │          │non-source│
 *     └─────────┬─────────┘          └─────┬─────┘               └────┬─────┘          └────┬─────┘
 *               │     StreamCompleted      │                          │                     │
 *               │─────────────────────────>│                          │                     │
 *               │                          │                          │                     │
 *               │                          ────┐                      │                     │
 *               │                              │ Wait for all sources │                     │
 *               │                          <───┘                      │                     │
 *               │                          │                          │                     │
 *               │                          │                          │                     │
 *               │╔═══════╤═════════════════╪══════════════════════════╪═════════════════════╪═══════════════════════════════╗
 *               │║ LOOP  │  until all are done                        │                     │                               ║
 *               │╟───────┘                 │                          │                     │                               ║
 *               │║         ╔═══════╤═══════╪══════════════════════════╪═══════════════╗     │                               ║
 *               │║         ║ LOOP  │  for all input vertices          │               ║     │                               ║
 *               │║         ╟───────┘       │                          │               ║     │                               ║
 *               │║         ║               │BroadcastStatusUpdateEvent│               ║     │                               ║
 *               │║         ║               │──────────────────────────>               ║     │                               ║
 *               │║         ╚═══════════════╪══════════════════════════╪═══════════════╝     │                               ║
 *               │║                         │                          │                     │                               ║
 *               │║                         │                          │ WorkingStatusUpdate │                               ║
 *               │║                         │                          │ ────────────────────>                               ║
 *               │║                         │                          │                     │                               ║
 *               │║                         │                          │                     │────┐                          ║
 *               │║                         │                          │                     │    │ Wait for all channels    ║
 *               │║                         │                          │                     │<───┘                          ║
 *               │║                         │                          │                     │                               ║
 *               │║                         │                          │                     │────┐                          ║
 *               │║                         │                          │                     │    │ Forward "Status-update"  ║
 *               │║                         │                          │                     │<───┘ message to sucessors     ║
 *               │║                         │                          │                     │                               ║
 *               │║                         │       "TaskWorkingStatus (DONE/NOT-YET)"       │                               ║
 *               │║                         │<────────────────────────────────────────────────                               ║
 *               │║                         │                          │                     │                               ║
 *               │║         ╔══════╤════════╪══════════════════════════╪═══════════════╗     │                               ║
 *               │║         ║ ALT  │  all-are-done                     │               ║     │                               ║
 *               │║         ╟──────┘        │                          │               ║     │                               ║
 *               │║         ║               │    StopInputTasks,end    │               ║     │                               ║
 *               │║         ║               │──────────────────────────>               ║     │                               ║
 *               │║         ╚═══════════════╪══════════════════════════╪═══════════════╝     │                               ║
 *               │╚═════════════════════════╪══════════════════════════╪═════════════════════╪═══════════════════════════════╝
 *     ┌─────────┴─────────┐          ┌─────┴─────┐               ┌────┴─────┐          ┌────┴─────┐
 *     │Stream Sources     │          │Coordinator│               │Heads and │          │Heads and │
 *     └───────────────────┘          └───────────┘               │Sources   │          │non-source│
 *                                                                └──────────┘          └──────────┘                             
 </pre>

 *the following state machine is the actial implementaion in this component
 *<pre>
 *	           ┌─────────────────┐┌─────────────────┐
 *	           │                 ││                 │
 *	           │                 ││           [Stop Timer]
 *	{a Streams Completed}        ▼▼            {timeout}
 *	           ▲              ┌───────┐             ▲
 *	           └──────────────│Waiting│─────────────┘
 *	                          └─────▲─┘
 *	                              │ │
 *	                              │ │
 *	                              │ └──────────────────────┐
 *	                   {All Streams Completed}             │
 *	                              │                        │
 *	┌────────────────────────▶[i<-i+1]                     │
 *	│                             │                        │
 *	│                             ▼
 *	│              [Broadcast Status Request : i]   {Job Restarts}
 *	│                     [Schedule Timer]
 *	│           ┌───────────────┐ │                        ▲
 *	│           │               │ │                        │
 *	│  {Operator Replied}       ▼ ▼                        │
 *	│           ▲          ┌────────────┐                  │
 *	│           └──────────│ Finalizing │──────────────────▶
 *	│                      └────────────┘                  │
 *	│                             │                        │
 *	│                             │                        │
 *	├──────────{timeout}──────────┤                        │
 *	│                             │                        │
 *	│                             │                        │
 *	│                  {All operators Replied}             │
 *	│                             │                        │
 *	│                             Λ                        │
 *	│                        No  ╱ ╲                       │
 *	└───────────────────────────done                       │
 *	                             ╲ ╱                       │
 *	                              V                        │
 *	                              │ yes                    │
 *	                              │                        │
 *	                   [Send Termination Msg]              │
 *	                        [Stop Timer]                   │
 *	                              │                        │
 *	                              ▼                        │
 *	                        ┌──────────┐                   │
 *	              ┌────────▶│Terminated│───────────────────┘
 *	              │         └──────────┘
 *	         [Stop Timer]         │
 *	              │               │
 *	         {timeout}            │
 *	              ▲               │
 *	              │               │
 *	              └───────────────┘
 *</pre>
 */
public class JobTerminationCoordinator implements JobStatusListener {

	enum TerminationState{WAITING,FINALIZING,TERMINATED};

	protected static final Logger LOG = LoggerFactory.getLogger(JobTerminationCoordinator.class);

	/**
	 * The tasks that this coordinator will ask to broadcast status message.
	 * These tasks are regular stream sources and iteration heads.
	 */
	private final Map<JobVertexID, ExecutionJobVertex> tasksToTrigger;
	/**
	 * The tasks that this coordinator will wait for their status reply.
	 * These are all tasks except regular stream sources
	 */
	private final Map<JobVertexID, ExecutionJobVertex> respondingTasks;
	/**
	 * The job that this coordinator is handling its termination
	 * */
	private final JobID jobId;
	/**
	 * The number of regular sources sub-tasks in this job.
	 */
	private final int numOfSourcesSubTasks;

	/**
	 * The set of finished regular stream sources
	 */
	private final Set<ExecutionAttemptID> finishedSources = new HashSet<>();

	/**
	 * The status of the termination
	 */
	private TerminationState status = TerminationState.WAITING;

	/**
	 * The status of each replying subtask, ready for termination or still busy.
	 */
	private final Map<ExecutionAttemptID, Boolean> respondingSubtasksStatus;
	/**
	 * a sequence number user to distinguish each status update round
	 */
	private int sequenceNumber  = 0;
	/**
	 * The total number of sub-tasks that this coordinator will wait for. All sub-tasks
	 * except regular stream sources' subtasks
	 */
	private final int expectedNumberOfResponses;
	/**
	 * a flag that determines wether there is loop(s) in the current job graph or not.
	 */
	private final boolean loopsExist;
	/**
	 * The timer that periodically checks the termination status.
	 */
	private  Timer timer ;
	/**
	 * the task being invoked by the timer.
	 */
	private TerminationTrigger terminationTrigger;
	/**
	 * How much should this coordinator waits for the sub-tasks replies.
	 */
	private long timerTimeout = 5_000;

	/**
	 * Lock used to handle synchronization in this coordinator
	 */
	private final Object lock = new Object();

	public JobTerminationCoordinator(JobID _jobId, Map<JobVertexID, ExecutionJobVertex> _allVertices){

		Preconditions.checkArgument(_allVertices != null &&
									_allVertices.size() > 0,"No vertices");
		this.tasksToTrigger =  new HashMap<>();
		this.respondingTasks = new HashMap<>();
		this.status = TerminationState.WAITING;
		boolean _loopsExist=false;
		int _numOfSources=0;
		int numberOfInputSubTasks=0;
		terminationTrigger = new TerminationTrigger();
		for(Map.Entry<JobVertexID, ExecutionJobVertex> e : _allVertices.entrySet()){
			if(e.getValue().getJobVertex().isSourceInputVertex()){
				tasksToTrigger.put(e.getKey(),e.getValue()); // sources
				_numOfSources += e.getValue().getTaskVertices().length;
			}else{
				respondingTasks.put(e.getKey(),e.getValue());
				if(e.getValue().getJobVertex().isInputVertex()){ // and not a source input vertex
					tasksToTrigger.put(e.getKey(),e.getValue()); // iteration head
					_loopsExist = true;
				}
			}
		}
		int numOfRespondingSubTask = 0;
		for(ExecutionJobVertex task : respondingTasks.values()){
			numOfRespondingSubTask+=task.getTaskVertices().length;
		}

		this.expectedNumberOfResponses = numOfRespondingSubTask;
		respondingSubtasksStatus = new HashMap<>();
		loopsExist = _loopsExist;
		this.jobId = _jobId;
		this.numOfSourcesSubTasks = _numOfSources;
	}

	/**
	 * The handler of stream source completion event. Starts the termination phase upon receiving
	 * {@link StreamCompleted} event from all stream sources
	 * @param event the stream completion event
	 */
	public void onStreamCompleted(StreamCompleted event){

		String taskName = getSubtaskName(tasksToTrigger,event);

		synchronized (lock) {

			onStreamCompletedPreconditions(event,taskName);

			LOG.info("Termination Coordinator received a stream completion event from  : {}, total : {}, done : {}",
					taskName, numOfSourcesSubTasks, finishedSources.size());

			JobID eventJobId = event.getJobID();
			JobVertexID taskVertexID = event.getTaskVertexID();
			ExecutionAttemptID sourceSubtaskId = event.getTaskExecutionId();

			if (tasksToTrigger.containsKey(taskVertexID)) { // we only listen to source vertices
				checkArgument(!finishedSources.contains(sourceSubtaskId),
						"Receiving an event twice for the same source; vertex: "+sourceSubtaskId+", name : "+taskName );

				finishedSources.add(sourceSubtaskId);

				if (finishedSources.size() == this.numOfSourcesSubTasks) {
					status = TerminationState.FINALIZING;
					if (!loopsExist) {
						terminate();
					} else {
						startNewSchedule();
					}
				}
			}
		}
	}

	/**
	 * The handler of
	 * @param taskStatus the event sent from running sub-tasks reporting their working status, idle or not.
	 */
	public void onTaskStatus(TaskWorkingStatus taskStatus){

		String taskName = getSubtaskName(respondingTasks,taskStatus);

		synchronized (lock) {
			if (taskStatus.getSequenceNumber() != sequenceNumber || status != TerminationState.FINALIZING) {
				LOG.info("task {} either terminated , status : {} or sequence number does not match {} <> {}", taskName,
						status, taskStatus.getSequenceNumber(), sequenceNumber);

				return;
			}

			checkArgument(this.jobId.equals(taskStatus.getJobID()),
					"Job ids ; current: {} and provided : {} does not match"
					, this.jobId, taskStatus.getJobID());

			if (respondingTasks.containsKey(taskStatus.getTaskVertexID())) {
				respondingSubtasksStatus.put(taskStatus.getTaskExecutionId(), taskStatus.isIdle());
				LOG.info("Total received responses at round {} are : {}/{}, reporting task : {} , idle : {}",
						sequenceNumber, respondingSubtasksStatus.size(), expectedNumberOfResponses, taskName,taskStatus.isIdle());
				if (respondingSubtasksStatus.size() == expectedNumberOfResponses) {
					onRoundClosure();
				}

			} else {
				LOG.error("error : receiving data from unknown vertex {} ", taskStatus.getTaskVertexID());
			}
		}
	}
	private void onStreamCompletedPreconditions(StreamCompleted event, String taskName){

		if(this.numOfSourcesSubTasks == 0){
			throw new IllegalStateException("Not expecting stream sources");
		}
		if (status == TerminationState.TERMINATED) {
			throw new IllegalStateException( "Receiving source finish event from " +
					"subtask "+taskName+" after termination!" );
		}
		if (finishedSources.size() >= this.numOfSourcesSubTasks) {
			//return;// should throw exception
			throw new IllegalStateException("Getting finished source events more than number of sources, SR : "
					+numOfSourcesSubTasks+", EVNTs : "+(numOfSourcesSubTasks+1));
		}

		JobID eventJobId = event.getJobID();

		checkArgument(this.jobId.equals(eventJobId), "Job ids ; current: {} and provided" +
				" : {} does not match", this.jobId, eventJobId);
	}
	/**
	 * Listener of the job execution state that resets its internal state upon getting a {@link JobStatus#RESTARTING}
	 * state
	 * @param jobId         The ID of the job.
	 * @param newJobStatus  The status the job switched to.
	 * @param timestamp     The timestamp when the status transition occurred.
	 * @param error         In case the job status switches to a failure state, this is the
	 */
	@Override
	public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
		LOG.info("termination coordinator responds to {}  state",newJobStatus);
		if(newJobStatus == JobStatus.RESTARTING){
			synchronized (lock) {
				restart();
			}
		}
	}

	private  void  askForStatus(){
		sequenceNumber++;
		sendUpdateEventTo(tasksToTrigger.values());
	}

	private void onRoundClosure() {
		LOG.info("Checking round closure!");
		synchronized (lock) {
			boolean allTasksDone = getAllStatus();
			if (allTasksDone) {
				terminate();

			}else {
				reset();
				askForStatus();
			}
		}
	}

	private boolean getAllStatus(){
		int numTrue=0;
		for(boolean b : respondingSubtasksStatus.values()){
			if(b){
				numTrue++;
			}
		}
		boolean allStatus = ( numTrue == expectedNumberOfResponses);
		LOG.info( "Round {} is done, all done : {} , ratio {}/{}",
				sequenceNumber,allStatus,numTrue,expectedNumberOfResponses);
		return allStatus;
	}

	private  void reset(){
		respondingSubtasksStatus.clear();
	}

	private	void  terminate(){
		synchronized (lock) {
			LOG.info("Sending a termination message to input vertices");
			sendStopEventTo(tasksToTrigger.values());
			status = TerminationState.TERMINATED;
			stopSchedule();
		}
	}

/*------------------------- UTILS for sending messages----------------------*/
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

	private void sendToSubTask(ExecutionVertex subtask, JobTerminationMessage msg){
		Execution execution = subtask.getCurrentExecutionAttempt();
		if (execution.getState() != ExecutionState.RUNNING) {
			LOG.info("Vertex {} is not running before get notified about ending stream, its state is {}",subtask,execution.getState());
		}
		ExecutionAttemptID id = execution.getAttemptId();
		subtask.sendMessageToCurrentExecution(msg, id);
	}

	private  void restart(){
		reset();
		stopSchedule(); // stop the old,if any!, the new schedule will start upon sources finished
		status = TerminationState.WAITING;
		finishedSources.clear();
		sequenceNumber++;
	}

	private void stopSchedule(){
		LOG.info("Stopping termination timer , status : {} ",status);
		if(terminationTrigger != null) {
			terminationTrigger.cancel();
		}
		terminationTrigger = null;
		if(timer != null){
				timer.cancel();
		}
		timer = null;
	}

	private void startNewSchedule(){
		stopSchedule();
		LOG.info("Starting new termination timer");
		terminationTrigger = new TerminationTrigger();
		timer =  new Timer("Job-Termination-Timer-"+jobId,true);
		timer.scheduleAtFixedRate(terminationTrigger,0,timerTimeout);
	}
	private void checkTermination(){
		synchronized (lock){
			if(status != TerminationState.FINALIZING){
				stopSchedule();
			}else{
				LOG.info("Termination round timed-out, will be (re)started ");
				onRoundClosure();
			}
		}

	}
	private String getSubtaskName(Map<JobVertexID, ExecutionJobVertex> inMap,SubtaskTerminationMessage msg){
		try {
			return inMap.get(msg.getTaskVertexID())
					.getTaskVertices()[msg.getSubtaskIndex()]
					.getTaskNameWithSubtaskIndex();
		}catch(Throwable e){
			return "";
		}

	}
	private class TerminationTrigger extends TimerTask {

		@Override
		public void run() {
			try {
				LOG.info("Termination timer fires now");
				checkTermination();
			} catch (Exception e) {
				LOG.error("Exception while triggering periodic termination check", e);
			}
		}
	}


}