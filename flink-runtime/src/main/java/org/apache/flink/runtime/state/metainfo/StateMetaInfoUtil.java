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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * Utility class for snapshot metadata.
 */
public class StateMetaInfoUtil {
	public static <K> SnapshotResult<StreamStateHandle> materializeMetaData(
		LocalRecoveryConfig localRecoveryConfig,
		long checkpointId,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry snapshotCloseableRegistry,
		TypeSerializer<K> keySerializer,
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) throws Exception {

		CheckpointStreamWithResultProvider streamWithResultProvider =

			localRecoveryConfig.isLocalRecoveryEnabled() ?

				CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					checkpointStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				CheckpointStreamWithResultProvider.createSimpleStream(
					CheckpointedStateScope.EXCLUSIVE,
					checkpointStreamFactory);

		snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

		try {
			//no need for compression scheme support because sst-files are already compressed
			KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(
					keySerializer,
					stateMetaInfoSnapshots,
					false);

			DataOutputView out =
				new DataOutputViewStreamWrapper(streamWithResultProvider.getCheckpointOutputStream());

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
}
