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

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.heap.HeapSnapshotResultCallable;
import org.apache.flink.runtime.state.heap.StateUID;
import org.apache.flink.runtime.state.heap.inc.IncrementalHeapSnapshotStrategy.PreviousSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;

class IncrementalHeapSnapshotResultCallable<K> extends HeapSnapshotResultCallable<K> {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalHeapSnapshotResultCallable.class);

	private final Optional<PreviousSnapshot> previousMaybe;
	private final long checkpointId;
	private final UUID backendIdentifier;
	private final Consumer<PreviousSnapshot> onSuccess;

	IncrementalHeapSnapshotResultCallable(
			SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
			KeyedBackendSerializationProxy<K> serializationProxy,
			Map<StateUID, StateSnapshot> cowStateStableSnapshots,
			Map<StateUID, Integer> stateNamesToId,
			KeyGroupRange keyGroupRange,
			StreamCompressionDecorator keyGroupCompressionDecorator,
			Consumer<Long> logAsyncSnapshotComplete,
			long checkpointId,
			Optional<PreviousSnapshot> previousMaybe,
			UUID backendIdentifier,
			Consumer<PreviousSnapshot> onSuccess) {
		super(
			checkpointStreamSupplier,
			serializationProxy,
			cowStateStableSnapshots,
			stateNamesToId,
			keyGroupRange,
			keyGroupCompressionDecorator,
			logAsyncSnapshotComplete);
		this.checkpointId = checkpointId;
		this.previousMaybe = previousMaybe;
		this.backendIdentifier = backendIdentifier;
		this.onSuccess = onSuccess;
	}

	@Override
	protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {
		SnapshotResult<KeyedStateHandle> newState = super.callInternal();
		SnapshotResult<StreamStateHandle> metaStateHandle = materializeMetaData();

		StreamStateHandle newJmState = (StreamStateHandle) newState.getJobManagerOwnedSnapshot(); // todo: remove cast
		StreamStateHandle newJmMetadata = metaStateHandle.getJobManagerOwnedSnapshot();
		StreamStateHandle newTlState = (StreamStateHandle) newState.getTaskLocalSnapshot(); // todo: remove cast
		StreamStateHandle newTlMetadata = metaStateHandle.getTaskLocalSnapshot();

		SnapshotResult<KeyedStateHandle> result =
			newTlMetadata == null || newTlState == null ?
				SnapshotResult.of(createOrUpdateSnapshot(newJmMetadata, newJmState)) :
				SnapshotResult.withLocalState(
					createOrUpdateSnapshot(newJmMetadata, newJmState),
					createOrUpdateSnapshot(newTlMetadata, newTlState));
		onSuccess.accept(new PreviousSnapshot(result, collectVersions(), previousMaybe.isPresent()));
		LOG.debug("Async-local phase finished for checkpoint {}", checkpointId);
		return result;
	}

	private IncrementalRemoteKeyedStateHandle createOrUpdateSnapshot(StreamStateHandle metaStateHandle, StreamStateHandle newSnapshot) {
		StateHandleID stateHandleID = new StateHandleID(Long.toString(checkpointId));
		LOG.debug("Create incremental snapshot for checkpoint {} {} from {}", checkpointId, newSnapshot, previousMaybe);
		HashMap<StateHandleID, StreamStateHandle> state = new HashMap<>();
		state.put(stateHandleID, newSnapshot);
		return previousMaybe
			.map(previous -> ((IncrementalRemoteKeyedStateHandle) previous.snapshot.getJobManagerOwnedSnapshot()).updated(
				state,
				checkpointId,
				metaStateHandle))
			.orElse(new IncrementalRemoteKeyedStateHandle(
				backendIdentifier,
				keyGroupRange,
				checkpointId,
				state,
				emptyMap(),
				metaStateHandle));
	}

	private SnapshotResult<StreamStateHandle> materializeMetaData() throws Exception {
		// todo: consider removing this -- metadata is already written in super
		// todo: deduplicate - see RocksDBIncrementalSnapshotOperation#callInternal

		CheckpointStreamWithResultProvider streamWithResultProvider = checkpointStreamSupplier.get();

		snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

		try {
			DataOutputView out = new DataOutputViewStreamWrapper(streamWithResultProvider.getCheckpointOutputStream());

			serializationProxy.write(out);

			if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
				SnapshotResult<StreamStateHandle> result = streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
				streamWithResultProvider = null;

				// Sanity checks - they should never fail
				Preconditions.checkNotNull(result, "Metadata was not properly created.");
				Preconditions.checkNotNull(result.getJobManagerOwnedSnapshot(), "Metadata for job manager was not properly created.");

				return result;
			} else {
				throw new IOException("Stream already closed and cannot return a handle.");
			}
		} finally {
			if (streamWithResultProvider != null) {
				if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
					IOUtils.closeQuietly(streamWithResultProvider);
				}
			}
		}
	}

	protected StateSnapshot.StateKeyGroupWriter getWriter(StateUID stateUID, int keyGroupId, StateSnapshot stateSnapshot) {
		if (stateSnapshot instanceof IncrementalStateTableSnapshot) {
			return (dov, keyGroupId1) -> {
				Preconditions.checkState(previousMaybe.isPresent());
				//noinspection rawtypes
				((IncrementalStateTableSnapshot) stateSnapshot).writeStateInKeyGroup(
					dov,
					keyGroupId1,
					// todo: pass versions as typed context to either writeStateInKeyGroup or to getKeyGroupWriter
					previousMaybe.get().mapVersions.getForState(stateUID).flatMap(mapVersions -> mapVersions.getVersion(keyGroupId1)));
			};
		} else {
			return super.getWriter(stateUID, keyGroupId, stateSnapshot);
		}
	}

	private StateTableVersions collectVersions() {
		HashMap<StateUID, StateMapVersions> stateVersions = new HashMap<>();
		for (Map.Entry<StateUID, StateSnapshot> e : cowStateStableSnapshots.entrySet()) {
			if (e.getValue() instanceof IncrementalStateTableSnapshot) {
				IncrementalStateTableSnapshot<?, ?, ?> s = (IncrementalStateTableSnapshot<?, ?, ?>) e.getValue();
				stateVersions.put(e.getKey(), s.collectVersions());
			}
		}
		return new StateTableVersions(stateVersions);
	}
}
