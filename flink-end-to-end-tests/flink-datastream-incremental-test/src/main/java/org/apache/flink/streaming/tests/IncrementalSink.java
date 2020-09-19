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
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IncrementalSink implements SinkFunction<TestEvent>, CheckpointedFunction {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalSink.class);

	private MapState<Integer, TestEvent> mapState;
	private final int timesToDelete;
	private final int serializerComputeIterations;

	IncrementalSink(int timesToDelete, int serializerComputeIterations) {
		this.timesToDelete = timesToDelete;
		this.serializerComputeIterations = serializerComputeIterations;
	}

	@Override
	public void invoke(TestEvent e, @SuppressWarnings("rawtypes") Context context) throws Exception {
		TestEvent testEvent = mapState.get(e.id);
		int timesSeen = testEvent == null ? 0 : testEvent.timesSeen;
		if (timesSeen == timesToDelete) {
			mapState.remove(e.id);
		} else {
			mapState.put(e.id, new TestEvent(e.id, timesSeen + 1, e.payload));
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) {
		// state is already updated
	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
		mapState = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
			"mapState",
			new IntSerializer(),
			new EventSerializer(serializerComputeIterations)));
	}
}
