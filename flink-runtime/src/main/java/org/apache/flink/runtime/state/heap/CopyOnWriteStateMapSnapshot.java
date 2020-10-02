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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * This class represents the snapshot of a {@link CopyOnWriteStateMap}.
 *
 * <p>IMPORTANT: Please notice that snapshot integrity of entries in this class rely on proper copy-on-write semantics
 * through the {@link CopyOnWriteStateMap} that created the snapshot object, but all objects in this snapshot must be considered
 * as READ-ONLY!. The reason is that the objects held by this class may or may not be deep copies of original objects
 * that may still used in the {@link CopyOnWriteStateMap}. This depends for each entry on whether or not it was subject to
 * copy-on-write operations by the {@link CopyOnWriteStateMap}. Phrased differently: the {@link CopyOnWriteStateMap} provides
 * copy-on-write isolation for this snapshot, but this snapshot does not isolate modifications from the
 * {@link CopyOnWriteStateMap}!
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class CopyOnWriteStateMapSnapshot<K, N, S>
	extends StateMapSnapshot<K, N, S, CopyOnWriteStateMap<K, N, S>> {

	/**
	 * Version of the {@link CopyOnWriteStateMap} when this snapshot was created. This can be used to release the snapshot.
	 */
	private final int snapshotVersion;

	/**
	 * The state map entries, as by the time this snapshot was created. Objects in this array may or may not be deep
	 * copies of the current entries in the {@link CopyOnWriteStateMap} that created this snapshot. This depends for each entry
	 * on whether or not it was subject to copy-on-write operations by the {@link CopyOnWriteStateMap}.
	 */
	@Nonnull
	private final CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData;

	/**
	 * Whether this snapshot has been released.
	 */
	private boolean released;

	/**
	 * Creates a new {@link CopyOnWriteStateMapSnapshot}.
	 *
	 * @param owningStateMap the {@link CopyOnWriteStateMap} for which this object represents a snapshot.
	 */
	CopyOnWriteStateMapSnapshot(CopyOnWriteStateMap<K, N, S> owningStateMap) {
		super(owningStateMap);

		this.snapshotData = owningStateMap.snapshotMapArrays();
		this.snapshotVersion = owningStateMap.getStateMapVersion();
		this.released = false;
	}

	@Override
	public void release() {
		if (!released) {
			owningStateMap.releaseSnapshot(this);
			released = true;
		}
	}

	public boolean isReleased() {
		return released;
	}

	/**
	 * Returns the internal version of the {@link CopyOnWriteStateMap} when this snapshot was created. This value must be used to
	 * tell the {@link CopyOnWriteStateMap} when to release this snapshot.
	 */
	int getSnapshotVersion() {
		return snapshotVersion;
	}

	@Override
	public void writeState(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer,
			@Nonnull DataOutputView dov,
			@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) throws IOException {
		iterate(stateSnapshotTransformer, dov::writeInt, entry -> entry.writeState(keySerializer, namespaceSerializer, stateSerializer, dov), Optional.empty());
	}

	protected void iterate(
			StateSnapshotTransformer<S> stateSnapshotTransformer,
			ThrowingConsumer<Integer, IOException> sizeWriter,
			ThrowingConsumer<CopyOnWriteStateMap.StateMapEntry<K, N, S>, IOException> entryWriter,
			Optional<Integer> minVersion) throws IOException {

		// if minVersion is set numberOfEntriesInSnapshotData will count for some irrelevant entries
		// todo: if minVersion and stateSnapshotTransformer both empty then can use numberOfEntriesInSnapshotData
		// todo: restore iterator.size encapsulation?
		// todo: optimize size calculation
		sizeWriter.accept(Iterators.size(new NonTransformSnapshotIterator<>(
			snapshotData,
			minVersion,
			Optional.ofNullable(stateSnapshotTransformer))));
		Iterator<StateEntry<K, N, S>> snapshotIterator = new NonTransformSnapshotIterator<>(
			snapshotData,
			minVersion,
			Optional.ofNullable(stateSnapshotTransformer));

		while (snapshotIterator.hasNext()) {
			entryWriter.accept((CopyOnWriteStateMap.StateMapEntry<K, N, S>) snapshotIterator.next());
		}
	}

	private static class NonTransformSnapshotIterator<K, N, S> implements Iterator<StateEntry<K, N, S>> {
		private final CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData;
		private final int minVersion;
		private CopyOnWriteStateMap.StateMapEntry<K, N, S> nextEntry;
		private int nextBucket = 0;
		private final StateSnapshotTransformer<S> transformer;

		NonTransformSnapshotIterator(
				CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData,
				Optional<Integer> minVersion,
				Optional<StateSnapshotTransformer<S>> stateSnapshotTransformer) {
			this.snapshotData = snapshotData;
			this.minVersion = minVersion.orElse(-1);
			this.transformer = stateSnapshotTransformer.orElse(null);
		}

		@Override
		public boolean hasNext() {
			advanceGlobally();
			return nextEntry != null;
		}

		@Override
		public CopyOnWriteStateMap.StateMapEntry<K, N, S> next() {
			advanceGlobally();
			if (nextEntry == null) {
				throw new NoSuchElementException();
			}
			CopyOnWriteStateMap.StateMapEntry<K, N, S> entry = nextEntry;
			nextEntry = nextEntry.next;
			return entry;
		}

		private void advanceGlobally() {
			advanceInChain();
			while (nextEntry == null && nextBucket < snapshotData.length) {
				nextEntry = snapshotData[nextBucket];
				advanceInChain();
				nextBucket++;
			}
		}

		private void advanceInChain() {
			while (nextEntry != null && (nextEntry.stateVersion < minVersion || filterOrTransformNextEntry() == null)) { // todo: check entryVersion too?
				nextEntry = nextEntry == null ? null : nextEntry.next; // can be null after filtering
			}
		}

		private CopyOnWriteStateMap.StateMapEntry<K, N, S> filterOrTransformNextEntry() {
			if (transformer != null && nextEntry != null) {
				S newValue = transformer.filterOrTransform(nextEntry.state);
				if (newValue == null) {
					nextEntry = null;
				} else if (newValue != nextEntry.state) {
					nextEntry = new CopyOnWriteStateMap.StateMapEntry<>(nextEntry, nextEntry.entryVersion);
				}
			}
			return nextEntry;
		}
	}
}
