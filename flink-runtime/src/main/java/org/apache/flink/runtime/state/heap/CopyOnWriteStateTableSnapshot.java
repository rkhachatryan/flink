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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.IncrementalHeapSnapshotStrategy.StateMapVersions;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * This class represents the snapshot of a {@link CopyOnWriteStateTable} and has a role in operator state checkpointing.
 * This class is also responsible for writing the state in the process of checkpointing.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
@Internal
public class CopyOnWriteStateTableSnapshot<K, N, S> extends AbstractStateTableSnapshot<K, N, S> {

	/**
	 * The offset to the contiguous key groups.
	 */
	private final int keyGroupOffset;

	/**
	 * Snapshots of state partitioned by key-group.
	 */
	@Nonnull
	private final List<CopyOnWriteStateMapSnapshot<K, N, S>> stateMapSnapshots;

	/**
	 * Creates a new {@link CopyOnWriteStateTableSnapshot}.
	 *
	 * @param owningStateTable the {@link CopyOnWriteStateTable} for which this object represents a snapshot.
	 */
	CopyOnWriteStateTableSnapshot(
		CopyOnWriteStateTable<K, N, S> owningStateTable,
		TypeSerializer<K> localKeySerializer,
		TypeSerializer<N> localNamespaceSerializer,
		TypeSerializer<S> localStateSerializer,
		StateSnapshotTransformer<S> stateSnapshotTransformer) {
		super(owningStateTable,
			localKeySerializer,
			localNamespaceSerializer,
			localStateSerializer,
			stateSnapshotTransformer);

		this.keyGroupOffset = owningStateTable.getKeyGroupOffset();
		this.stateMapSnapshots = owningStateTable.getStateMapSnapshotList();
	}

	@Override
	protected CopyOnWriteStateMapSnapshot<K, N, S> getStateMapSnapshotForKeyGroup(int keyGroup) {
		int indexOffset = keyGroup - keyGroupOffset;
		CopyOnWriteStateMapSnapshot<K, N, S> stateMapSnapshot = null;
		if (indexOffset >= 0 && indexOffset < stateMapSnapshots.size()) {
			stateMapSnapshot = stateMapSnapshots.get(indexOffset);
		}

		return stateMapSnapshot;
	}

	@Override
	public void release() {
		for (CopyOnWriteStateMapSnapshot<K, N, S> snapshot : stateMapSnapshots) {
			if (!snapshot.isReleased()) {
				snapshot.release();
			}
		}
	}

	public StateMapVersions collectVersions() {
		final Map<Integer, Integer> versions = new HashMap<>();
		final ListIterator<CopyOnWriteStateMapSnapshot<K, N, S>> i = stateMapSnapshots.listIterator();
		while (i.hasNext()) {
			versions.put(
				i.nextIndex() + keyGroupOffset,
				i.next().getSnapshotVersion());
		}
		return new StateMapVersions(versions);
	}

}
