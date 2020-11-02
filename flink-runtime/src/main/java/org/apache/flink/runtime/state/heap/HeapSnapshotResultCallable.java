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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult;

/**
 * {@link AsyncSnapshotCallable} for heap backend.
 */
@Internal
public class HeapSnapshotResultCallable<K> extends AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> {
	private static final Logger LOG = LoggerFactory.getLogger(HeapSnapshotResultCallable.class);

	protected final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier;
	protected final KeyedBackendSerializationProxy<K> serializationProxy;
	protected final Map<StateUID, StateSnapshot> cowStateStableSnapshots;
	private final Map<StateUID, Integer> stateNamesToId;
	protected final KeyGroupRange keyGroupRange;
	private final StreamCompressionDecorator keyGroupCompressionDecorator;
	private final Consumer<Long> logAsyncSnapshotComplete;

	protected HeapSnapshotResultCallable(
			SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
			KeyedBackendSerializationProxy<K> serializationProxy,
			Map<StateUID, StateSnapshot> cowStateStableSnapshots,
			Map<StateUID, Integer> stateNamesToId,
			KeyGroupRange keyGroupRange,
			StreamCompressionDecorator keyGroupCompressionDecorator,
			Consumer<Long> logAsyncSnapshotComplete) {
		this.checkpointStreamSupplier = checkpointStreamSupplier;
		this.serializationProxy = serializationProxy;
		this.cowStateStableSnapshots = cowStateStableSnapshots;
		this.stateNamesToId = stateNamesToId;
		this.keyGroupRange = keyGroupRange;
		this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
		this.logAsyncSnapshotComplete = logAsyncSnapshotComplete;
	}

	@Override
	protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {
		final CheckpointStreamWithResultProvider streamWithResultProvider = checkpointStreamSupplier.get();

		snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

		final CheckpointStateOutputStream localStream = streamWithResultProvider.getCheckpointOutputStream();

		final DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);
		serializationProxy.write(outView);

		final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

		Map<StateUID, Long> times = new HashMap<>();
		Map<StateUID, Long> sizes  = new HashMap<>();
		for (int keyGroupPos = 0; keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos) {
			int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
			keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
			outView.writeInt(keyGroupId);

			for (Map.Entry<StateUID, StateSnapshot> stateSnapshot : cowStateStableSnapshots.entrySet()) {
				long startPos = localStream.getPos();
				long startTime = System.nanoTime();
				StateSnapshot.StateKeyGroupWriter partitionedSnapshot = stateSnapshot.getValue().getKeyGroupWriter();
				try (OutputStream kgCompressionOut = keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
					DataOutputViewStreamWrapper kgCompressionView = new DataOutputViewStreamWrapper(kgCompressionOut);
					kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
					kgCompressionView.writeBoolean(partitionedSnapshot.isIncremental());
					writeSnapshot(keyGroupId, partitionedSnapshot, kgCompressionView, stateSnapshot.getKey());
					times.put(stateSnapshot.getKey(), times.getOrDefault(stateSnapshot.getKey(), 0L) + (System.nanoTime() - startTime) / 1000_000);
					sizes.put(stateSnapshot.getKey(), sizes.getOrDefault(stateSnapshot.getKey(), 0L) + (localStream.getPos() - startPos) / 1024);
				} // this will just close the outer compression stream
			}
		}
		times.forEach((id, value) -> LOG.debug("Written {}Kb of state '{}' in {}ms", sizes.get(id), id.getStateName(), value));

		if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
			KeyGroupRangeOffsets kgOffs = new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
			SnapshotResult<StreamStateHandle> result = streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
			return toKeyedStateHandleSnapshotResult(result, kgOffs);
		} else {
			throw new IOException("Stream already unregistered.");
		}
	}

	protected void writeSnapshot(int keyGroupId, StateSnapshot.StateKeyGroupWriter partitionedSnapshot, DataOutputViewStreamWrapper kgCompressionView, StateUID stateUID) throws IOException {
		partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
	}

	@Override
	protected void cleanupProvidedResources() {
		for (StateSnapshot tableSnapshot : cowStateStableSnapshots.values()) {
			tableSnapshot.release();
		}
	}

	@Override
	protected void logAsyncSnapshotComplete(long startTime) {
		logAsyncSnapshotComplete.accept(startTime);
	}
}
