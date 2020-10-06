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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.dstl.inmemory.InMemoryLogClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * {@link InMemoryLogClient} test.
 */
@RunWith(Parameterized.class)
public class LogClientTest {

	private final Random random = new Random();
	private final LogClientType logClientType;

	@Parameterized.Parameters(name = "{0}")
	public static Object[][] parameters() {
		return new Object[][]{
			{LogClientType.MEMORY}
		};
	}

	public LogClientTest(LogClientType logClientType) {
		this.logClientType = logClientType;
	}

	@Test
	public void testWriteAndRead() throws ExecutionException, InterruptedException {
		OperatorID operatorID = new OperatorID();
		int keyGroup = 0;
		int keyLen = 10;
		int valueLen = 100;
		int numAppends = 20;
		List<Tuple2<byte[], byte[]>> data = Stream.generate(() -> Tuple2.of(bytes(keyLen), bytes(valueLen))).limit(numAppends).collect(Collectors.toList());
		long timestamp = System.currentTimeMillis();

		LogClient client = logClientType.createLogClient();
		LogWriter writer = client.createWriter(operatorID, KeyGroupRange.of(keyGroup, keyGroup));

		SequenceNumber after = writer.lastAppendedSqn();
		data.forEach(el -> writer.append(keyGroup, el.f0, el.f1, timestamp));
		SequenceNumber until = writer.lastAppendedSqn();

		LogPointer logPointer = writer.persistUntil(after, until).get();
		CloseableIterator<LogRecord> it = client.replay(logPointer, after, until, KeyGroupRange.of(keyGroup, keyGroup));
		assertEquals(toLogRecords(data, keyGroup, timestamp), ImmutableList.copyOf(it));
	}

	private List<LogRecord> toLogRecords(List<Tuple2<byte[], byte[]>> data, int keyGroup, long timestamp) {
		return data.stream().map(t -> LogRecord.of(keyGroup, timestamp, t.f0, t.f1)).collect(Collectors.toList());
	}

	private byte[] bytes(int len) {
		byte[] bytes = new byte[len];
		random.nextBytes(bytes);
		return bytes;
	}

	@Test(expected = IllegalStateException.class)
	public void testNoAppendAfterClose() {
		LogWriter writer = logClientType.createLogClient().createWriter(new OperatorID(), KeyGroupRange.of(0, 0));
		writer.close();
		writer.append(0, new byte[0], new byte[0], 0);
	}

	private enum LogClientType {
		MEMORY {
			@Override
			public LogClient createLogClient() {
				return new InMemoryLogClient();
			}
		};

		public abstract LogClient createLogClient();
	}
}
