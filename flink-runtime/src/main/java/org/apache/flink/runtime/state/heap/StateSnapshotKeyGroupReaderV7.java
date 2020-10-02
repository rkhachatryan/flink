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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.heap.inc.StateDiff;
import org.apache.flink.runtime.state.heap.inc.StateDiffSerializer;

import java.io.IOException;

class StateSnapshotKeyGroupReaderV7<K, N, S> implements StateSnapshotKeyGroupReader {
	private final StateTable<K, N, S> stateTable;

	StateSnapshotKeyGroupReaderV7(StateTable<K, N, S> stateTable) {
		this.stateTable = stateTable;
	}

	@Override
	public void readMappingsInKeyGroup(DataInputView in, int keyGroupId) throws IOException {
		final TypeSerializer<K> keySerializer = stateTable.keySerializer;
		final TypeSerializer<N> namespaceSerializer = stateTable.getNamespaceSerializer();
		final TypeSerializer<S> stateSerializer = stateTable.getStateSerializer();
		//noinspection unchecked
		final StateDiffSerializer<S, StateDiff<S>> diffSerializer = (StateDiffSerializer<S, StateDiff<S>>) stateTable.getMetaInfo().getIncrementalStateMetaInfo().getDiffSerializer();
		int numElements;
		numElements = in.readInt();
		for (int i = 0; i < numElements; i++) {
			final N namespace = namespaceSerializer.deserialize(in);
			final K key = keySerializer.deserialize(in);
			StateMap<K, N, S> stateMap = stateTable.getMapForKeyGroup(keyGroupId);
			{
				stateMap.put(key, namespace, diffSerializer.deserialize(in).apply(stateMap.get(key, namespace))); // todo: use transform
			}
		}
		{
			numElements = in.readInt();
			for (int j = 0; j < numElements; j++) {
				final K key = keySerializer.deserialize(in);
				final N namespace = namespaceSerializer.deserialize(in);
				stateTable.getMapForKeyGroup(keyGroupId).remove(key, namespace);
			}
		}
	}
}
