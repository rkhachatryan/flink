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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.dstl.LogId;
import org.apache.flink.dstl.LogPointer;
import org.apache.flink.dstl.LogRecord;
import org.apache.flink.dstl.SequenceNumber;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

// todo low: document (file structure & alg)
class LogRecordStreamIterator implements CloseableIterator<LogRecord> {

	// format: logCount, [ logId, [ sqnCount, [ sqn, kgCount, [ kg, byteCount, bytes ] ] ] ]
	private final LogStreamReader logStreamReader;
	private final FSDataInputStream is;
	private final LogRecordFilter filter;
	private int numLogsInFile = -1, numSqnsInLog = -1, numKeyGroupsInSqn = -1;
	private int currentLogIndex, currentSqnIndex, currentKgIndex;
	private LogRecord currentRecord;
	private LogId currentLogId;
	private SequenceNumber currentSqn;

	LogRecordStreamIterator(FSDataInputStream is, LogRecordFilter filter) {
		this.is = is;
		this.logStreamReader = new LogStreamReader(is);
		this.filter = filter;
	}

	@Override
	public boolean hasNext() {
		advanceIfNeeded();
		return currentRecord != null;
	}

	@Override
	public LogRecord next() {
		checkState(hasNext());
		LogRecord tmp = currentRecord;
		currentRecord = null;
		return tmp;
	}

	@Override
	public void close() throws Exception {
		is.close();
	}

	private void advanceIfNeeded() {
		while (canAdvance() && (currentRecord == null || skipCurrentRecord())) {
			try {
				advanceRecord();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			// todo low: skip bytes instead of records (need to calculate and write size)
		}
	}

	private void advanceRecord() throws IOException {
		if (currentKgIndex >= numKeyGroupsInSqn) {
			advanceSqn();
		}
		if (canAdvanceKeyGroup()) {
			currentKgIndex++;
			int keyGroup = logStreamReader.readKeyGroup();
			byte[] bytes = logStreamReader.readValue(); // todo low: read into pre-allocated memory
			currentRecord = LogRecord.of(keyGroup, bytes); // todo low: reuse object
		}
	}

	private void advanceSqn() throws IOException {
		if (currentSqnIndex >= numSqnsInLog) {
			advanceLog();
		}
		if (canAdvanceSqn()) {
			currentSqnIndex++;
			currentSqn = logStreamReader.readSequenceNumber();
			numKeyGroupsInSqn = logStreamReader.readCount();
		}
	}

	private void advanceLog() throws IOException {
		if (needReadNumLogs()) {
			numLogsInFile = logStreamReader.readCount();
		}
		if (canAdvanceLog()) {
			currentLogIndex++;
			currentLogId = logStreamReader.readLogId();
			numSqnsInLog = logStreamReader.readCount();
		}
	}

	private boolean canAdvance() {
		return canAdvanceKeyGroup() || canAdvanceSqn() || (canAdvanceLog() || needReadNumLogs());
	}

	private boolean needReadNumLogs() {
		return numLogsInFile < 0;
	}

	private boolean canAdvanceLog() {
		return currentLogIndex < numLogsInFile;
	}

	private boolean canAdvanceSqn() {
		return currentSqnIndex < numSqnsInLog;
	}

	private boolean canAdvanceKeyGroup() {
		return currentKgIndex < numKeyGroupsInSqn;
	}

	private boolean skipCurrentRecord() {
		return !filter.includes(currentLogId, currentSqn, currentRecord);
	}

	static class LogRecordFilter {
		private final LogPointer logPointer;
		private final SequenceNumber from;
		private final SequenceNumber to;
		private final KeyGroupRange keyGroupRange;

		LogRecordFilter(LogPointer logPointer, SequenceNumber from, SequenceNumber to, KeyGroupRange keyGroupRange) {
			this.logPointer = logPointer;
			this.from = from;
			this.to = to;
			this.keyGroupRange = keyGroupRange;
		}

		private boolean includes(LogId id, SequenceNumber sqn, LogRecord logRecord) {
			return logPointer.logId().equals(id) && keyGroupRange.contains(logRecord.keyGroup()) && SequenceNumber.contains(from, to, sqn);
		}
	}
}
