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

package org.apache.flink.runtime.state.heap.inc;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapRestoreOperation;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Incremental {@link HeapRestoreOperation}.
 */
public class IncrementalHeapRestoreOperation<K> extends HeapRestoreOperation<K> {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalHeapRestoreOperation.class);

	public IncrementalHeapRestoreOperation(
		@Nonnull Collection<KeyedStateHandle> restoreStateHandles,
		StateSerializerProvider<K> keySerializerProvider,
		ClassLoader userCodeClassLoader,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		CloseableRegistry cancelStreamRegistry,
		HeapPriorityQueueSetFactory priorityQueueSetFactory,
		@Nonnull KeyGroupRange keyGroupRange,
		int numberOfKeyGroups,
		HeapSnapshotStrategy<K> snapshotStrategy,
		InternalKeyContext<K> keyContext) {
		super(
			restoreStateHandles,
			keySerializerProvider,
			userCodeClassLoader,
			registeredKVStates,
			registeredPQStates,
			cancelStreamRegistry,
			priorityQueueSetFactory,
			keyGroupRange,
			numberOfKeyGroups,
			snapshotStrategy,
			keyContext);
	}

	@Override
	protected void restoreHandle(boolean isTheFirst, KeyedStateHandle keyedStateHandle) throws IOException, StateMigrationException {
		if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			LOG.trace("Restore incremental handle: {} (first: {})", keyedStateHandle, isTheFirst);
			List<Map.Entry<StateHandleID, StreamStateHandle>> sorted = ((IncrementalRemoteKeyedStateHandle) keyedStateHandle)
				.getSharedState()
				.entrySet()
				.stream()
				.sorted(Comparator.comparing(entry -> Long.parseLong(entry.getKey().getKeyString())))
				.collect(Collectors.toList());
			for (Map.Entry<StateHandleID, StreamStateHandle> e: sorted) {
				LOG.trace("Restore incremental part: {}", e.getKey());
				super.restoreHandle(isTheFirst, (KeyedStateHandle) e.getValue());
				isTheFirst = false;
			}
		} else {
			LOG.trace("Restore non-incremental handle: {} (first: {})", keyedStateHandle, isTheFirst);
			super.restoreHandle(isTheFirst, keyedStateHandle);
		}
	}

	@Override
	protected RegisteredKeyValueStateBackendMetaInfo<?, ?> createMetaInfo(StateMetaInfoSnapshot metaInfoSnapshot) {
		return new IncrementalRegisteredKeyValueStateBackendMetaInfo<>(metaInfoSnapshot);
	}
}
