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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.inc.StateDiff;
import org.apache.flink.runtime.state.heap.inc.StateDiffSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class IncrementalStateMapSnapshot<K, N, S> extends CopyOnWriteStateMapSnapshot<K, N, S> {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalStateMapSnapshot.class);

	private final Collection<Map<K, Set<N>>> removed;

	IncrementalStateMapSnapshot(CopyOnWriteStateMap<K, N, S> owningStateMap) {
		super(owningStateMap);
		this.removed = owningStateMap.snapshotRemovedKeys();
	}

	void writeStateDiff(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			StateDiffSerializer<S, StateDiff<S>> diffSerializer,
			@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer,
			@Nonnull DataOutputView dov,
			Optional<Integer> minVersion) throws IOException {
		long start = System.currentTimeMillis();
		Map<K, Set<N>> distinctRemoved = distinct(removed);
		LOG.trace("Removal deduplication took {}ms", System.currentTimeMillis() - start);
		start = System.currentTimeMillis();
		iterate(stateSnapshotTransformer, dov::writeInt, entry -> {
			entry.writeStateDiff(keySerializer, namespaceSerializer, diffSerializer, dov);
			removeRemoval(entry, distinctRemoved);
		}, minVersion);
		LOG.trace("Writing entries took {}ms", System.currentTimeMillis() - start);
		start = System.currentTimeMillis();
		writeRemoved(keySerializer, namespaceSerializer, dov, distinctRemoved);
		LOG.trace("Writing removals took {}ms", System.currentTimeMillis() - start);
	}

	private void writeRemoved(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, DataOutputView dov, Map<K, Set<N>> removedUnique) throws IOException {
		dov.writeInt(size(removedUnique));
		for (Map.Entry<K, Set<N>> entry : removedUnique.entrySet()) {
			for (N n : entry.getValue()) {
				keySerializer.serialize(entry.getKey(), dov);
				namespaceSerializer.serialize(n, dov);
			}
		}
	}

	private void removeRemoval(CopyOnWriteStateMap.StateMapEntry<K, N, S> entry, Map<K, Set<N>> distinctRemoved) {
		Set<N> ns = distinctRemoved.get(entry.key);
		if (ns != null) {
			ns.remove(entry.namespace);
		}
	}

	private static  <K, N> Map<K, Set<N>> distinct(Collection<Map<K, Set<N>>> removed) {
		Map<K, Set<N>> unique = new HashMap<>();
		for (Map<K, Set<N>> entries : removed) {
			unique.putAll(entries);
		}
		return unique;
	}

	private static <K, N> int size(Map<K, Set<N>> removed) {
		int size = 0;
		for (Set<N> entries : removed.values()) {
			size += entries.size();
		}
		return size;
	}

}
