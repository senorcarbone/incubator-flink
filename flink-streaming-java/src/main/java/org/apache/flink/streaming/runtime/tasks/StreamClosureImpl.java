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


import org.apache.flink.runtime.iterative.termination.JobTerminationCoordinator;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a hock class that is started upon finishing a stream source. It starts after notifying the coordinator
 * {@link JobTerminationCoordinator} and blocks until its stop method is called.
 */
public class StreamClosureImpl implements  StreamClosure {

	protected static final Logger LOG = LoggerFactory.getLogger(StreamClosureImpl.class);

	private  volatile   boolean finalizing = true;

	Object lock;

	public StreamClosureImpl() {
	}

	@Override
	public   void waitUntilFinalized(){
		Preconditions.checkState(Thread.holdsLock(lock),"Should be called while holding the specified lock");
		if(!finalizing){
			throw new IllegalStateException("Asked to wait while not finalizing");
		}
		// awaiting until finalization complete
		while(finalizing) { // spin lock (loop) for spurious wake-ups
			try {
				LOG.info("Waiting until coordinator finalizes");
				lock.wait();
				LOG.info("Waiting done");
			} catch (InterruptedException e) {
				break; // interrupted .. should terminate
			}
		}

	}
	@Override
	public  synchronized void stop() {
		if(!finalizing) {
			LOG.error("Stream termination closure is not in a finalization state");
		}
		LOG.info("Stopping stream closure");
		this.finalizing = false;
		lock.notifyAll();
	}

	@Override
	public void setLock(Object o) {
		this.lock = o;
	}

	@Override
	public boolean isWaiting() {
		return finalizing;
	}
}