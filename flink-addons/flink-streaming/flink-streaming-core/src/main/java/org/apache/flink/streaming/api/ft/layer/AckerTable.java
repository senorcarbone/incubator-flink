/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft.layer;

import java.io.Serializable;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckerTable implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AckerTable.class);

	protected FTLayer ftLayer;

	protected HashMap<Long, Long> ackTable;

	protected AckerTable(FTLayer ftLayer) {
		this.ftLayer = ftLayer;
		ackTable = new HashMap<Long, Long>();
	}

	public synchronized void newSourceRecord(long sourceRecordId) {
		ackTable.put(sourceRecordId, 0L);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Added to AckerTable: ({},{})", Long.toHexString(sourceRecordId), 0);
		}
	}

	public synchronized void xor(long sourceRecordId, long recordId) {
		try {
			Long ackValue = ackTable.get(sourceRecordId) ^ recordId;
			if (LOG.isDebugEnabled()) {
				LOG.debug("XORed to AckerTable: ({},{})", Long.toHexString(sourceRecordId),
						Long.toHexString(recordId));
				LOG.debug("Ack value after XOR: ({},{})", Long.toHexString(sourceRecordId),
						Long.toHexString(ackValue));
			}
			if (ackValue == 0L) {
				ftLayer.ack(sourceRecordId);
			} else {
				ackTable.put(sourceRecordId, ackValue);
			}
		} catch (NullPointerException e) {
			throw new RuntimeException("No such record (" + Long.toHexString(sourceRecordId)
					+ ") in AckerTable");
		}
	}

	public synchronized void remove(long sourceRecordId) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Removed from AckerTable: ({},{})", Long.toHexString(sourceRecordId),
					Long.toHexString(ackTable.get(sourceRecordId)));
		}
		ackTable.remove(sourceRecordId);

		if (ackTable.isEmpty()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("AckerTable is FINISHED");
			}
		}
	}

	public boolean isEmpty() {
		return ackTable.isEmpty();
	}

	public boolean contains(long sourceRecordId) {
		return ackTable.containsKey(sourceRecordId);
	}
}
