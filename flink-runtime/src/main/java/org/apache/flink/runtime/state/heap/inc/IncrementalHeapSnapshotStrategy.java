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

import org.apache.flink.api.common.typeutils.TypeSerializer;
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
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
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
public class IncrementalHeapSnapshotStrategy<K> extends HeapSnapshotStrategy<K> {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalHeapSnapshotStrategy.class);

	private final UUID backendIdentifier;
	private long lastConfirmedCheckpoint = Long.MIN_VALUE;
	private PreviousSnapshot lastConfirmedSnapshot;
	private SortedMap<Long, PreviousSnapshot> unconfirmedSnapshots = new TreeMap<>();
	private final int maxIncremental = Integer.parseInt(System.getProperty("heap-max-inc-snapshots", "100")); // todo: read from config

	public IncrementalHeapSnapshotStrategy(
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		LocalRecoveryConfig localRecoveryConfig,
		KeyGroupRange keyGroupRange,
		CloseableRegistry cancelStreamRegistry,
		StateSerializerProvider<K> keySerializerProvider,
		UUID backendIdentifier) {
		super(
			synchronicityTrait(),
			registeredKVStates,
			registeredPQStates,
			keyGroupCompressionDecorator,
			localRecoveryConfig,
			keyGroupRange,
			cancelStreamRegistry,
			keySerializerProvider);
		LOG.info("Initialized {} with maxIncremental checkpoints: {}", getClass().getSimpleName(), maxIncremental);
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

		boolean incremental = cowStateStableSnapshots.values().stream().anyMatch(s -> s instanceof IncrementalCopyOnWriteStateTableSnapshot);

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
		boolean incrementalWanted = checkpointId % maxIncremental != 0; // using modulo to synchronize all subtasks to prevent skew in checkpoint sizes
		boolean incrementalAllowed = !checkpointOptions.getCheckpointType().isSavepoint() && supportsIncrementalSnapshots(state);
		boolean incrementalPossible = checkpointId == lastConfirmedCheckpoint + 1; // todo: do we also need a full checkpoint after savepoint because journals will be lost? (old comment)
		boolean incrementalDecided = incrementalWanted && incrementalAllowed && incrementalPossible;
		LOG.trace("Snapshot (sync) '{}' for checkpoint {} (incremental: {})", state, checkpointId, incrementalDecided);
		if (incrementalWanted && incrementalAllowed && !incrementalPossible) {
			LOG.debug("incremental snapshot of {} impossible for checkpoint {} (last confirmed: {})",  // todo: state name
				System.identityHashCode(state), checkpointId, lastConfirmedCheckpoint);
		}
		return incrementalDecided ?
			((IncrementalStateSnapshotRestore) state).incrementalStateSnapshot() :
			super.getStateSnapshot(state, checkpointId, checkpointOptions);
	}

	@SuppressWarnings("rawtypes")
	private boolean supportsIncrementalSnapshots(StateSnapshotRestore state) {
		return
			state instanceof IncrementalStateSnapshotRestore &&
			state instanceof StateTable &&
			// supposedly, mutability of BinaryRowData keys cause blink-planner e2e failures
			// state name: window-aggs, type: VALUE, BinaryRowData, BinaryRowDataSerializer
			// symptoms: division by zero or hash mismatch without logging removals
			// todo: investigate further; copy keys in removal log if not immutable?
			((StateTable) state).getKeySerializer().isImmutableType();
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

	/**
	 * todo: make non-public.
	 */
	// todo: move to upper level
	public static class StateMapVersions {
		private final Map<Integer, Integer> kgMapVersion;

		public StateMapVersions(Map<Integer, Integer> kgMapVersion) {
			this.kgMapVersion = kgMapVersion;
		}

		public Optional<Integer> getVersion(int keyGroup) {
			return Optional.ofNullable(kgMapVersion.get(keyGroup));
		}

		@Override
		public String toString() {
			return "kgMapVersion=" + kgMapVersion;
		}
	}

	// todo: move to upper level
	static class StateTableVersions {
		StateTableVersions(Map<StateUID, StateMapVersions> stateVersions) {
			this.stateVersions = stateVersions;
		}

		private final Map<StateUID, StateMapVersions> stateVersions;

		public Optional<StateMapVersions> getForState(StateUID stateUID) {
			return Optional.ofNullable(stateVersions.get(stateUID));
		}

		@Override
		public String toString() {
			return "stateVersions=" + stateVersions;
		}
	}

	static class PreviousSnapshot {
		final SnapshotResult<KeyedStateHandle> snapshot;
		final StateTableVersions mapVersions;

		PreviousSnapshot(SnapshotResult<KeyedStateHandle> snapshot, StateTableVersions mapVersions) {
			this.snapshot = snapshot;
			this.mapVersions = mapVersions;
		}

		@Override
		public String toString() {
			return "snapshot=" + snapshot + ", mapVersions=" + mapVersions;
		}
	}

	private static <K> SnapshotStrategySynchronicityBehavior<K> synchronicityTrait() {
		return new SnapshotStrategySynchronicityBehavior<K>() {
			@Override
			public boolean isAsynchronous() {
				return true;
			}

			@Override
			public <N, V> StateTable<K, N, V> newStateTable(InternalKeyContext<K> keyContext, RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo, TypeSerializer<K> keySerializer) {
				return new IncrementalCopyOnWriteStateTable<>(keyContext, (IncrementalRegisteredKeyValueStateBackendMetaInfo<N, V>) newMetaInfo, keySerializer);
			}
		};
	}
}
