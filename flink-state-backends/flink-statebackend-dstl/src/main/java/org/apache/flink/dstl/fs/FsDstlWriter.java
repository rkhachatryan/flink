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

package org.apache.flink.dstl.fs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.dstl.LogId;
import org.apache.flink.dstl.LogId.UuidLogId;
import org.apache.flink.dstl.LogPointer;
import org.apache.flink.dstl.LogWriter;
import org.apache.flink.dstl.SequenceNumber;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@NotThreadSafe
class FsDstlWriter implements LogWriter {
	private static final Logger LOG = LoggerFactory.getLogger(FsDstlWriter.class);

	private final FsLogPersister persister;
	private final UuidLogId logId;
	private final Deque<Tuple2<SequenceNumber, List<LogFragment>>> sealed = new ArrayDeque<>();
	private List<LogFragment> current = new ArrayList<>();
	private SequenceNumber currentSqn = SequenceNumber.of(0L);
	private boolean closed;

	FsDstlWriter(FsLogPersister persister, UuidLogId logId) {
		this.persister = persister;
		this.logId = logId;
	}

	@Override
	public LogId logId() {
		return logId;
	}

	@Override
	public void append(int keyGroup, byte[] key, byte[] value, long timestamp) {
		// todo: check size threshold and trigger persist if needed
		Preconditions.checkState(!closed, "LogWriter is closed");
		LOG.debug("append to {}: keyGroup={}, timestamp={} {} bytes", logId, keyGroup, timestamp, value.length);
		current.add(new LogFragment(keyGroup, value));
	}

	@Override
	public SequenceNumber lastAppendedSqn() {
		SequenceNumber temp = currentSqn;
		rollover();
		return temp;
	}

	@Override
	public CompletableFuture<LogPointer> persistUntil(SequenceNumber from, SequenceNumber to) {
		LOG.debug("persistUntil log {}: from={}, to={}", logId, from, to);
		Preconditions.checkNotNull(from);
		Preconditions.checkNotNull(to);
		rollover();
		return persister.persist(logId, snapshot(from, to));
	}

	// drop anything before from - will not be needed - todo: confirm
	// drain and return everything between from and to
	// keep everything from to
	private List<Tuple2<SequenceNumber, List<LogFragment>>> snapshot(SequenceNumber from, SequenceNumber to) {
		List<Tuple2<SequenceNumber, List<LogFragment>>> result = new ArrayList<>();
		while (!sealed.isEmpty() && sealed.peek().f0.compareTo(to) < 0) {
			Tuple2<SequenceNumber, List<LogFragment>> element = sealed.poll();
			if (element.f0.compareTo(from) >= 0) {
				result.add(element);
			}
		}
		return result;
	}

	@Override
	public void close() {
		LOG.debug("close log {}", logId);
		Preconditions.checkState(!closed);
		closed = true;
		sealed.clear();
		current.clear();
	}

	private void rollover() {
		sealed.add(Tuple2.of(currentSqn, current));
		current = new ArrayList<>();
		currentSqn = currentSqn.next(); // todo: review sqn increment cases
	}
}
