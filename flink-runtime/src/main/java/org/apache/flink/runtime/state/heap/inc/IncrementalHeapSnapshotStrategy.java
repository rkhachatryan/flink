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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;
import org.apache.flink.runtime.state.heap.SnapshotStrategySynchronicityBehavior;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.heap.StateUID;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.flink.runtime.state.heap.StateUID.of;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType.KEY_VALUE;

/**
 * Incremental {@link HeapSnapshotStrategy}.
 */
@Internal
public class IncrementalHeapSnapshotStrategy<K> extends HeapSnapshotStrategy<K> {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalHeapSnapshotStrategy.class);

	private final UUID backendIdentifier;
	private long lastConfirmedCheckpoint = Long.MIN_VALUE;
	private PreviousSnapshot lastConfirmedSnapshot;
	private SortedMap<Long, PreviousSnapshot> unconfirmedSnapshots = new TreeMap<>();
	private final int maxIncremental = 5; // todo: read from config
	private int numIncremental = 0;

	public IncrementalHeapSnapshotStrategy(
			SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait,
			Map<String, StateTable<K, ?, ?>> registeredKVStates,
			Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
			StreamCompressionDecorator keyGroupCompressionDecorator,
			LocalRecoveryConfig localRecoveryConfig,
			KeyGroupRange keyGroupRange,
			CloseableRegistry cancelStreamRegistry,
			StateSerializerProvider<K> keySerializerProvider,
			UUID backendIdentifier) {
		super(
			snapshotStrategySynchronicityTrait,
			registeredKVStates,
			registeredPQStates,
			keyGroupCompressionDecorator,
			localRecoveryConfig,
			keyGroupRange,
			cancelStreamRegistry,
			keySerializerProvider);
		this.backendIdentifier = backendIdentifier;
	}

	@Override
	protected AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> getSnapshotCallable(
			long checkpointId,
			CheckpointStreamFactory primaryStreamFactory,
			Map<StateUID, Integer> stateNamesToId,
			Map<StateUID, StateSnapshot> cowStateStableSnapshots,
			KeyedBackendSerializationProxy<K> serializationProxy,
			SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
			CheckpointOptions checkpointOptions) {

		boolean incremental = cowStateStableSnapshots.values().stream().anyMatch(s -> s instanceof IncrementalStateTableSnapshot);

		LOG.debug("Snapshot (async) for checkpoint {} (incremental: {})", checkpointId, incremental);
		return new IncrementalHeapSnapshotResultCallable<>(
			checkpointStreamSupplier,
			serializationProxy,
			cowStateStableSnapshots,
			stateNamesToId,
			keyGroupRange,
			keyGroupCompressionDecorator,
			startTime -> {
				if (snapshotStrategySynchronicityTrait.isAsynchronous()) {
					logAsyncCompleted(primaryStreamFactory, startTime);
				}
			},
			checkpointId,
			incremental ? Optional.of(lastConfirmedSnapshot) : Optional.empty(),
			backendIdentifier,
			snapshot -> {
				synchronized (IncrementalHeapSnapshotStrategy.this) { // todo: efficiency: optimize synchronization
					unconfirmedSnapshots.put(checkpointId, snapshot);
				}
			});
	}

	@Override
	protected StateSnapshot getStateSnapshot(StateSnapshotRestore state, long checkpointId, CheckpointOptions checkpointOptions) {
		// note: after savepoint we also need a full checkpoint because journals will be lost
		// todo: increment numIncremental here? currently, incrementing on completion - can lead to exceeding the limit
		boolean canSnapshotIncrementally = numIncremental < maxIncremental && !checkpointOptions.getCheckpointType().isSavepoint() && checkpointId == lastConfirmedCheckpoint + 1 && state instanceof IncrementalCopyOnWriteStateTable;
		LOG.trace("Snapshot (sync) '{}' for checkpoint {} (incremental: {})", state.stateSnapshot().getMetaInfoSnapshot().getName(), checkpointId, canSnapshotIncrementally);
		return canSnapshotIncrementally ?
			// todo: remove cast
			((IncrementalCopyOnWriteStateTable) state).incrementalStateSnapshot() :
			super.getStateSnapshot(state, checkpointId, checkpointOptions);
	}

	@Override
	public void checkpointConfirmed(long id) {
		super.checkpointConfirmed(id);
		synchronized (IncrementalHeapSnapshotStrategy.this) {
			PreviousSnapshot confirmed = unconfirmedSnapshots.remove(id);
			if (id > lastConfirmedCheckpoint && confirmed != null) { // null if savepoint todo: confirm
				lastConfirmedCheckpoint = id;
				lastConfirmedSnapshot = confirmed;
				LOG.debug("Update last confirmed checkpoint to {} {}", lastConfirmedCheckpoint, lastConfirmedSnapshot);
				unconfirmedSnapshots = unconfirmedSnapshots.tailMap(id);
				release(confirmed);
				if (confirmed.incremental) {
					numIncremental++;
				} else {
					numIncremental = 0;
				}
			}
		}
	}

	private void release(PreviousSnapshot snapshot) {
		super.registeredKVStates.forEach(
			(stateName, stateTable) ->
				snapshot.mapVersions.getForState(of(stateName, KEY_VALUE /* see HeapSnapshotStrategy.processSnapshotMetaInfoForAllStates */))
					.ifPresent(versions -> versions.kgMapVersion.forEach(stateTable::confirmSnapshot)));
	}

	protected CheckpointedStateScope getCheckpointedStateScope() {
		return CheckpointedStateScope.SHARED;
	}

	static class PreviousSnapshot {
		final SnapshotResult<KeyedStateHandle> snapshot;
		final StateTableVersions mapVersions;
		final boolean incremental;

		PreviousSnapshot(SnapshotResult<KeyedStateHandle> snapshot, StateTableVersions mapVersions, boolean incremental) {
			this.snapshot = snapshot;
			this.mapVersions = mapVersions;
			this.incremental = incremental;
		}

		@Override
		public String toString() {
			return "snapshot=" + snapshot + ", mapVersions=" + mapVersions;
		}
	}
}
