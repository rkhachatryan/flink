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
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMap;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateTable;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.StateMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Incremental {@link CopyOnWriteStateTable}.
 */
public class IncrementalCopyOnWriteStateTable<K, N, S> extends CopyOnWriteStateTable<K, N, S> {
	public IncrementalCopyOnWriteStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		super(keyContext, metaInfo, keySerializer);
	}

	@Override
	public void confirmSnapshot(int keyGroup, int version) {
		getMapForKeyGroup(keyGroup).confirmSnapshot(version);
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

	public List<IncrementalStateMapSnapshot<K, N, S>> getIncrementalStateMapSnapshotList() {
		return getSnapshots(IncrementalCopyOnWriteStateMap::incrementalStateSnapshot);
	}

	// todo: deduplicate
	private <T> List<T> getSnapshots(Function<IncrementalCopyOnWriteStateMap<K, N, S>, T> snapshot) {
		List<T> snapshotList = new ArrayList<>(keyGroupedStateMaps.length);
		for (StateMap<K, N, S> map : keyGroupedStateMaps) {
			snapshotList.add(snapshot.apply((IncrementalCopyOnWriteStateMap<K, N, S>) map));
		}
		return snapshotList;
	}

	@Override
	protected CopyOnWriteStateMap<K, N, S> createStateMap() {
		return new IncrementalCopyOnWriteStateMap(getStateSerializer(), metaInfo.getIncrementalStateMetaInfo().getJournalFactory());
	}

}
