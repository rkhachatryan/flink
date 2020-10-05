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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMapSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;

class IncrementalStateTableSnapshot<K, N, S> implements StateSnapshot {

	private final StateDiffSerializer<S, StateDiff<S>> diffSerializer;
	private final int keyGroupOffset;
	private final TypeSerializer<K> localKeySerializer;
	private final TypeSerializer<N> localNamespaceSerializer;
	private final StateSnapshotTransformer<S> stateSnapshotTransformer;
	private final StateMetaInfoSnapshot metaInfoSnapshot;
	private final List<IncrementalStateMapSnapshot<K, N, S>> stateMapSnapshots;

	IncrementalStateTableSnapshot(
		IncrementalCopyOnWriteStateTable<K, N, S> owningStateTable,
		TypeSerializer<K> localKeySerializer,
		TypeSerializer<N> localNamespaceSerializer,
		StateSnapshotTransformer<S> stateSnapshotTransformer,
		StateDiffSerializer<S, StateDiff<S>> diffSerializer) {
		this.diffSerializer = diffSerializer;
		this.metaInfoSnapshot = owningStateTable.metaInfo.snapshot();
		this.keyGroupOffset = owningStateTable.getKeyGroupOffset();
		this.localKeySerializer = Preconditions.checkNotNull(localKeySerializer);
		this.localNamespaceSerializer = Preconditions.checkNotNull(localNamespaceSerializer);
		this.stateSnapshotTransformer = stateSnapshotTransformer;
		this.stateMapSnapshots = owningStateTable.getIncrementalStateMapSnapshotList();
	}

	public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId, Optional<Integer> minVersion) throws IOException {
		IncrementalStateMapSnapshot<K, N, S> stateMapSnapshot = getStateMapSnapshotForKeyGroup(keyGroupId);
		dov.writeBoolean(true); // incremental
		stateMapSnapshot.writeStateDiff(localKeySerializer, localNamespaceSerializer, diffSerializer, stateSnapshotTransformer, dov, minVersion);
		stateMapSnapshot.release();
	}

	@Override
	public StateKeyGroupWriter getKeyGroupWriter() {
		throw new UnsupportedOperationException();
	}

	@Override
	public StateMetaInfoSnapshot getMetaInfoSnapshot() {
		return this.metaInfoSnapshot;
	}

	@Override
	public void release() {
		for (CopyOnWriteStateMapSnapshot<K, N, S> snapshot : stateMapSnapshots) {
			if (!snapshot.isReleased()) {
				snapshot.release();
			}
		}
	}

	private IncrementalStateMapSnapshot<K, N, S> getStateMapSnapshotForKeyGroup(int keyGroup) {
		int indexOffset = keyGroup - keyGroupOffset;
		IncrementalStateMapSnapshot<K, N, S> stateMapSnapshot = null;
		if (indexOffset >= 0 && indexOffset < stateMapSnapshots.size()) {
			stateMapSnapshot = stateMapSnapshots.get(indexOffset);
		}

		return stateMapSnapshot;
	}

	public StateMapVersions collectVersions() {
		final Map<Integer, Integer> versions = new HashMap<>();
		final ListIterator<IncrementalStateMapSnapshot<K, N, S>> i = stateMapSnapshots.listIterator();
		while (i.hasNext()) {
			versions.put(
				i.nextIndex() + keyGroupOffset,
				i.next().getSnapshotVersion());
		}
		return new StateMapVersions(versions);
	}

}
