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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.StateMigrationException;

import java.util.Map;

/**
 * Incremental {@link HeapKeyedStateBackend}.
 */
public class IncrementalHeapKeyedStateBackend<K> extends HeapKeyedStateBackend<K> {

	public IncrementalHeapKeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			ClassLoader userCodeClassLoader,
			ExecutionConfig executionConfig,
			TtlTimeProvider ttlTimeProvider,
			CloseableRegistry cancelStreamRegistry,
			StreamCompressionDecorator keyGroupCompressionDecorator,
			Map<String, StateTable<K, ?, ?>> registeredKVStates,
			Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
			LocalRecoveryConfig localRecoveryConfig,
			HeapPriorityQueueSetFactory priorityQueueSetFactory,
			HeapSnapshotStrategy<K> snapshotStrategy,
			InternalKeyContext<K> keyContext) {
		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistry,
			keyGroupCompressionDecorator,
			registeredKVStates,
			registeredPQStates,
			localRecoveryConfig,
			priorityQueueSetFactory,
			snapshotStrategy,
			keyContext);
	}

	@Override
	protected <N, V> void updateMetaInfo(
			RegisteredKeyValueStateBackendMetaInfo<N, V> restoredKvMetaInfo,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<?, V> stateDesc,
			StateSnapshotTransformFactory<V> snapshotTransformFactory,
			TypeSerializer<V> newStateSerializer) throws StateMigrationException {
		super.updateMetaInfo(restoredKvMetaInfo, namespaceSerializer, stateDesc, snapshotTransformFactory, newStateSerializer);
		((IncrementalRegisteredKeyValueStateBackendMetaInfo<N, V>) restoredKvMetaInfo).setIncrementalStateMetaInfo(IncrementalStateMetaInfo.fromStateDescriptor(stateDesc)); // fixme
	}

	@Override
	protected <N, V> IncrementalRegisteredKeyValueStateBackendMetaInfo<N, V> createMetaInfo(
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<?, V> stateDesc,
			StateSnapshotTransformFactory<V> snapshotTransformFactory,
			TypeSerializer<V> newStateSerializer) {
		return new IncrementalRegisteredKeyValueStateBackendMetaInfo<>(stateDesc, namespaceSerializer, newStateSerializer, snapshotTransformFactory);
	}
}
