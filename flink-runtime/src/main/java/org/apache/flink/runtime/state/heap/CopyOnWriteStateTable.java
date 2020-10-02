/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.inc.StateDiff;
import org.apache.flink.runtime.state.heap.inc.StateDiffSerializer;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * This implementation of {@link StateTable} uses {@link CopyOnWriteStateMap}. This implementation supports asynchronous snapshots.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
public class CopyOnWriteStateTable<K, N, S> extends StateTable<K, N, S> {

	/**
	 * Constructs a new {@code CopyOnWriteStateTable}.
	 *
	 * @param keyContext    the key context.
	 * @param metaInfo      the meta information, including the type serializer for state copy-on-write.
	 * @param keySerializer the serializer of the key.
	 */
	CopyOnWriteStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		super(keyContext, metaInfo, keySerializer);
	}

	@Override
	protected CopyOnWriteStateMap<K, N, S> createStateMap() {
		return new CopyOnWriteStateMap(getStateSerializer(), metaInfo.getIncrementalStateMetaInfo().getJournalFactory());
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	/**
	 * Creates a snapshot of this {@link CopyOnWriteStateTable}, to be written in checkpointing.
	 *
	 * @return a snapshot from this {@link CopyOnWriteStateTable}, for checkpointing.
	 */
	@Nonnull
	@Override
	public CopyOnWriteStateTableSnapshot<K, N, S> stateSnapshot() {
		return new CopyOnWriteStateTableSnapshot<>(
			this,
			getKeySerializer().duplicate(),
			getNamespaceSerializer().duplicate(),
			getStateSerializer().duplicate(),
			getMetaInfo().getStateSnapshotTransformFactory().createForDeserializedState().orElse(null));
	}

	@SuppressWarnings("unchecked")
	public IncrementalStateTableSnapshot<K, N, S> incrementalStateSnapshot() {
		return new IncrementalStateTableSnapshot<>(
			this,
			getKeySerializer().duplicate(),
			getNamespaceSerializer().duplicate(),
			getMetaInfo().getStateSnapshotTransformFactory().createForDeserializedState().orElse(null),
			(StateDiffSerializer<S, StateDiff<S>>) getMetaInfo().getIncrementalStateMetaInfo().getDiffSerializer());
	}

	List<CopyOnWriteStateMapSnapshot<K, N, S>> getStateMapSnapshotList() {
		return getSnapshots(CopyOnWriteStateMap::stateSnapshot);
	}

	List<IncrementalStateMapSnapshot<K, N, S>> getIncrementalStateMapSnapshotList() {
		return getSnapshots(CopyOnWriteStateMap::incrementalStateSnapshot);
	}

	private <T> List<T> getSnapshots(Function<CopyOnWriteStateMap<K, N, S>, T> snapshot) {
		List<T> snapshotList = new ArrayList<>(keyGroupedStateMaps.length);
		for (StateMap<K, N, S> map : keyGroupedStateMaps) {
			snapshotList.add(snapshot.apply((CopyOnWriteStateMap<K, N, S>) map));
		}
		return snapshotList;
	}
}
