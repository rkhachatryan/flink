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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

class IncrementalOperator extends AbstractStreamOperator<Void> implements OneInputStreamOperator<TestEvent, Void> {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalOperator.class);

	private MapState<Integer, TestEvent> mapState;
	private final int timesToDelete;
	private final int serializerComputeIterations;
	private final int stateSize;
	private final int payloadLength;
	private final Random random;

	IncrementalOperator(int timesToDelete, int serializerComputeIterations, int stateSize, int payloadLength) {
		this.timesToDelete = timesToDelete;
		this.serializerComputeIterations = serializerComputeIterations;
		this.stateSize = stateSize;
		this.payloadLength = payloadLength;
		this.random = new Random();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		long start = System.currentTimeMillis();
		int count = 0;
		int parallelism = getExecutionConfig().getParallelism();
		int maxParallelism = getExecutionConfig().getMaxParallelism();
		int curIdx = getContainingTask().getIndexInSubtaskGroup();
		LOG.debug("parallelism: {}, maxParallelism: {}, curIdx: {}", parallelism, maxParallelism, curIdx);
		mapState = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
			"mapState",
			new IntSerializer(),
			new EventSerializer(serializerComputeIterations)));
		if (context.isRestored()) {
			throw new UnsupportedOperationException("context.isRestored() is unsupported");
		} else {
			for (int i = 0; i < stateSize; i++) {
				int reqIdx = org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignKeyToParallelOperator(i, maxParallelism, parallelism);
				boolean use = curIdx == reqIdx;
				if (use) {
					count++;
					setCurrentKey(i);
					mapState.put(i, new TestEvent(i, 1, getPayload()));
				}
			}
		}
		LOG.info("initialized state with {} elements in {}ms", count, System.currentTimeMillis() - start);
	}

	@Override
	public void processElement(StreamRecord<TestEvent> element) throws Exception {
		TestEvent e = element.getValue();
		TestEvent testEvent = mapState.get(e.id);
		int timesSeen = testEvent == null ? 0 : testEvent.timesSeen;
		if (timesSeen == timesToDelete) {
			mapState.remove(e.id);
		} else {
			mapState.put(e.id, new TestEvent(e.id, timesSeen + 1, e.payload));
		}
	}

	private byte[] getPayload() {
		byte[] payload = new byte[payloadLength];
		random.nextBytes(payload);
		return payload;
	}
}
