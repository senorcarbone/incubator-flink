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

package org.apache.flink.runtime.io.network.api.reader;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.flink.runtime.event.task.StreamingSuperstep;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BarrierBuffer {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);

	private Queue<BufferOrEvent> bufferOrEvents = new LinkedList<BufferOrEvent>();
	private Queue<BufferOrEvent> unprocessed = new LinkedList<BufferOrEvent>();

	private Set<Integer> blockedChannels = new HashSet<Integer>();
	private int totalNumberOfInputChannels;

	private StreamingSuperstep currentSuperstep;
	private boolean receivedSuperstep;

	private boolean blockAll = false;

	private AbstractReader reader;

	public BarrierBuffer(InputGate inputGate, AbstractReader reader) {
		totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.reader = reader;
	}

	public void startSuperstep(StreamingSuperstep superstep) {
		this.currentSuperstep = superstep;
		this.receivedSuperstep = true;
		if (LOG.isDebugEnabled()) {
			LOG.debug("Superstep started with id: " + superstep.getId());
		}
	}

	public void store(BufferOrEvent bufferOrEvent) {
		bufferOrEvents.add(bufferOrEvent);
	}

	public BufferOrEvent getNonProcessed() {
		return unprocessed.poll();
	}

	public boolean isBlocked(int channelIndex) {
		return blockAll || blockedChannels.contains(channelIndex);
	}

	public void blockAll() {
		this.blockAll = true;
	}

	public void releaseAllBlock() {
		this.blockAll = false;
	}

	public boolean containsNonprocessed() {
		return !unprocessed.isEmpty();
	}

	public boolean receivedSuperstep() {
		return receivedSuperstep;
	}

	public void blockChannel(int channelIndex) {
		if (!blockedChannels.contains(channelIndex)) {
			blockedChannels.add(channelIndex);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Channel blocked with index: " + channelIndex);
			}
			if (blockedChannels.size() == totalNumberOfInputChannels) {
				reader.publish(currentSuperstep);
				unprocessed.addAll(bufferOrEvents);
				bufferOrEvents.clear();
				blockedChannels.clear();
				receivedSuperstep = false;
				if (LOG.isDebugEnabled()) {
					LOG.debug("All barriers received, blocks released");
				}
			}

		} else {
			throw new RuntimeException("Tried to block an already blocked channel");
		}
	}

	public String toString() {
		return blockedChannels.toString();
	}

}