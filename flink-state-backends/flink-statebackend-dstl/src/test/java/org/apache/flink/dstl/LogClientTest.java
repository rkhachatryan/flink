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

package org.apache.flink.dstl;

import org.apache.flink.core.fs.Path;
import org.apache.flink.dstl.fs.FsLogClient;
import org.apache.flink.dstl.inmemory.InMemoryLogClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.CloseableIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;
import static org.junit.Assert.assertEquals;

/**
 * {@link InMemoryLogClient} test.
 */
@RunWith(Parameterized.class)
public class LogClientTest {

	private final Random random = new Random();
	private final LogClientType clientType;

	@Parameterized.Parameters(name = "{0}")
	public static Object[] parameters() {
		return LogClientType.values();
	}

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public LogClientTest(LogClientType logClientType) {
		this.clientType = logClientType;
	}

	@Test(expected = IllegalStateException.class)
	public void testNoAppendAfterClose() throws IOException {
		LogWriter writer = clientType.create(this).createWriter(new OperatorID(), KeyGroupRange.of(0, 0));
		writer.close();
		writer.append(0, new byte[0], new byte[0], 0);
	}

	@Test
	public void testWriteAndRead() throws Exception {
		byte[] key = new byte[0]; // ignore keys for now
		long timestamp = System.currentTimeMillis(); // ignore timestamp for now
		KeyGroupRange kgRange = KeyGroupRange.of(0, 5);
		Map<Integer, List<byte[]>> appendsByKeyGroup = generateAppends(kgRange, 10, 20);

		try (LogClient client = clientType.create(this); LogWriter writer = client.createWriter(new OperatorID(), kgRange)) {
			SequenceNumber from = writer.lastAppendedSqn();
			appendsByKeyGroup.forEach((group, appends) -> appends.forEach(bytes -> writer.append(group, key, bytes, timestamp)));

			SequenceNumber to = writer.lastAppendedSqn().next(); // exclusive
			LogPointer pointer = writer.persistUntil(from, to).get();

			// todo medium: request group by group and in narrower range to verify filtering
			try (CloseableIterator<LogRecord> replay = client.replay(pointer, from, to, kgRange)) {
				assertEquals(
					toLogRecords(appendsByKeyGroup, key, timestamp),
					mergeRecords(replay, timestamp, key));
			}
		}
	}

	private Map<Integer, List<byte[]>> generateAppends(KeyGroupRange kgRange, int keyLen, int appendsPerGroup) {
		return stream(kgRange.spliterator(), false).collect(toMap(identity(), unused -> generateData(appendsPerGroup, keyLen)));
	}

	private List<byte[]> generateData(int numAppends, int keyLen) {
		return Stream.generate(() -> randomBytes(keyLen)).limit(numAppends).collect(Collectors.toList());
	}

	private byte[] randomBytes(int len) {
		byte[] bytes = new byte[len];
		random.nextBytes(bytes);
		return bytes;
	}

	private static List<LogRecord> mergeRecords(CloseableIterator<LogRecord> records, long timestamp, byte[] key) {
		Map<Integer, List<byte[]>> groupedByKeyGroup = new HashMap<>();
		while (records.hasNext()) {
			LogRecord record = records.next();
			groupedByKeyGroup
				.computeIfAbsent(record.keyGroup(), unused -> new ArrayList<>())
				.add(record.value());
		}
		return toLogRecords(groupedByKeyGroup, key, timestamp);
	}

	private static List<LogRecord> toLogRecords(Map<Integer, List<byte[]>> valuesByKeyGroup, byte[] key, long timestamp) {
		return valuesByKeyGroup.entrySet().stream()
			.map(e -> LogRecord.of(e.getKey(), timestamp, key, mergeBytes(e.getValue())))
			.collect(Collectors.toList());
	}

	private static byte[] mergeBytes(List<byte[]> values) {
		byte[] merged = new byte[values.stream().mapToInt(t -> t.length).sum()];
		int pos = 0;
		for (byte[] value : values) {
			System.arraycopy(value, 0, merged, pos, value.length);
			pos += value.length;
		}
		return merged;
	}

	private enum LogClientType {
		MEMORY {
			@Override
			public LogClient create(LogClientTest logClientTest) {
				return new InMemoryLogClient();
			}
		},
		FILE {
			@Override
			public LogClient create(LogClientTest logClientTest) throws IOException {
				File folder = logClientTest.temporaryFolder.newFolder();
				folder.deleteOnExit();
				return new FsLogClient(Path.fromLocalFile(folder), 100, 1, 100);
			}
		};

		public abstract LogClient create(LogClientTest logClientTest) throws IOException;
	}
}
