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

package org.apache.flink.streaming.tests;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;

import java.util.Random;

class TestEventSource implements SourceFunction<TestEvent> {

	private final int idRange;
	private final int payloadLength;
	private final int sleepPerBatchMs;
	private final int numElementsRequired;
	private final int batchSize;
	private int numElementsSent;

	private volatile boolean isRunning = true;
	private final Random random = new Random();

	TestEventSource(int range, int count, int batchSize, int sleepPerBatchMs, int payloadLength) {
		Preconditions.checkArgument(count > 0);
		Preconditions.checkArgument(range > 0);
		this.batchSize = batchSize > 1 ? batchSize : count;
		this.sleepPerBatchMs = sleepPerBatchMs;
		this.idRange = range;
		this.payloadLength = payloadLength;
		this.numElementsRequired = count;
	}

	@Override
	public void run(SourceContext<TestEvent> ctx) throws InterruptedException {
		while (isActive()) {
			for (int i = 0; (isActive() && (i < batchSize || batchSize <= 0)); i++) {
				ctx.collect(new TestEvent(random.nextInt(idRange), 0, getPayload()));
				numElementsSent++;
			}
			if (sleepPerBatchMs > 0) {
				Thread.sleep(sleepPerBatchMs);
			}
		}
	}

	private boolean isActive() {
		return isRunning && (numElementsSent < numElementsRequired || numElementsRequired <= 0);
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	private byte[] getPayload() {
		byte[] payload = new byte[payloadLength];
		random.nextBytes(payload);
		return payload;
	}

}
