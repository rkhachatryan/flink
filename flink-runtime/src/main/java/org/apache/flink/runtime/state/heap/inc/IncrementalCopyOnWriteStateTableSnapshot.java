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
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMapSnapshot;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateTable;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateTableSnapshot;

import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

/**
 * Incremental {@link CopyOnWriteStateTableSnapshot}.
 */
public class IncrementalCopyOnWriteStateTableSnapshot<K, N, S> extends CopyOnWriteStateTableSnapshot<K, N, S> {

	IncrementalCopyOnWriteStateTableSnapshot(
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
