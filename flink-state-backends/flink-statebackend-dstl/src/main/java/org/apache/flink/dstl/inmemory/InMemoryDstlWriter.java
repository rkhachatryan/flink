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

package org.apache.flink.dstl.inmemory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.dstl.LogId;
import org.apache.flink.dstl.LogPointer;
import org.apache.flink.dstl.LogRecord;
import org.apache.flink.dstl.LogWriter;
import org.apache.flink.dstl.SequenceNumber;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@NotThreadSafe
class InMemoryDstlWriter implements LogWriter {
	private static final Logger LOG = LoggerFactory.getLogger(InMemoryDstlWriter.class);

	private final LogId logId;
	private final Map<Integer, List<Tuple2<SequenceNumber, LogRecord>>> log = new HashMap<>();
	private long sqn = 0L;
	private boolean closed;

	InMemoryDstlWriter(LogId logId) {
		this.logId = logId;
	}

	@Override
	public LogId logId() {
		return logId;
	}

	@Override
	public void append(int keyGroup, byte[] key, byte[] value, long timestamp) {
		Preconditions.checkState(!closed, "LogWriter is closed");
		LOG.debug("append to {}: keyGroup={}, timestamp={} {} bytes", logId, keyGroup, timestamp, value.length);
		log.computeIfAbsent(keyGroup, unused -> new ArrayList<>()).add(Tuple2.of(SequenceNumber.of(sqn++), LogRecord.of(keyGroup, timestamp, key, value)));
	}

	@Override
	public SequenceNumber lastAppendedSqn() {
		return SequenceNumber.of(sqn);
	}

	@Override
	public CompletableFuture<LogPointer> persistUntil(SequenceNumber after, SequenceNumber until) {
		LOG.debug("persistUntil log {}: after={}, until={}", logId, after, until);
		Preconditions.checkNotNull(after);
		Preconditions.checkNotNull(until);
		return CompletableFuture.completedFuture(LogPointer.of(logId));
	}

	@Override
	public void close() {
		LOG.debug("close log {}", logId);
		Preconditions.checkState(!closed);
		closed = true;
	}

	CloseableIterator<LogRecord> replay(SequenceNumber after, SequenceNumber until, KeyGroupRange keyGroupRange) {
		Preconditions.checkArgument(after.compareTo(until) <= 0);
		if (keyGroupRange.getNumberOfKeyGroups() == 0) {
			return CloseableIterator.empty();
		}
		return CloseableIterator.adapterForIterator(log.entrySet().stream()
			.filter(e -> keyGroupRange.contains(e.getKey()))
			.map(Map.Entry::getValue)
			.flatMap(Collection::stream)
			.filter(e -> e.f0.compareTo(after) >= 0 && e.f0.compareTo(until) <= 0)
			.map(e -> e.f1)
			.iterator());
	}
}
