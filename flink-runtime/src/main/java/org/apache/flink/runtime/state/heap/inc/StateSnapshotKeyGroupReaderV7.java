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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * StateSnapshotKeyGroupReader that supports incremental snapshots.
 */
public class StateSnapshotKeyGroupReaderV7<K, N, S> implements StateSnapshotKeyGroupReader {
	private final IncrementalCopyOnWriteStateTable<K, N, S> stateTable;

	public StateSnapshotKeyGroupReaderV7(IncrementalCopyOnWriteStateTable<K, N, S> stateTable) {
		this.stateTable = stateTable;
	}

	@Override
	public void readMappingsInKeyGroup(DataInputView in, int keyGroupId) throws IOException {
		TypeSerializer<K> keySerializer = stateTable.getKeySerializer();
		TypeSerializer<N> namespaceSerializer = stateTable.getNamespaceSerializer();
		TypeSerializer<S> stateSerializer = stateTable.getStateSerializer();
		readDiffs(in, keyGroupId, keySerializer, namespaceSerializer);
		readRemovals(in, keyGroupId, keySerializer, namespaceSerializer);
	}

	private void readDiffs(DataInputView in, int keyGroupId, TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer) throws IOException {
		//noinspection unchecked
		final StateDiffSerializer<S, StateDiff<S>> diffSerializer = (StateDiffSerializer<S, StateDiff<S>>) stateTable.getMetaInfo().getIncrementalStateMetaInfo().getDiffSerializer();
		StateMap<K, N, S> stateMap = stateTable.getMapForKeyGroup(keyGroupId);
		while (in.readBoolean()) {
			K key = keySerializer.deserialize(in);
			N namespace = namespaceSerializer.deserialize(in);
			StateDiff<S> stateDiff = diffSerializer.deserialize(in);
			S state = stateDiff.apply(stateMap.get(key, namespace));
			stateMap.put(key, namespace, state); // todo: use transform
		}
	}

	private void readRemovals(DataInputView in, int keyGroupId, TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer) throws IOException {
		int numElements = in.readInt();
		for (int i = 0; i < numElements; i++) {
			K key = Preconditions.checkNotNull(keySerializer.deserialize(in));
			N namespace = Preconditions.checkNotNull(namespaceSerializer.deserialize(in));
			stateTable.getMapForKeyGroup(keyGroupId).remove(key, namespace);
		}
	}
}
