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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;

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
	private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteStateMapSnapshot.class);

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

	/** The number of (non-null) entries in snapshotData. */
	@Nonnegative
	private final int numberOfEntriesInSnapshotData;

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
		this.numberOfEntriesInSnapshotData = owningStateMap.size();
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
		if (stateSnapshotTransformer != null && minVersion.isPresent()) {
			throw new UnsupportedOperationException();
		}

		Supplier<Iterator<StateEntry<K, N, S>>> snapshotIteratorSupplier = () -> stateSnapshotTransformer == null ?
			new NonTransformSnapshotIterator<>(snapshotData, minVersion) :
			new TransformedSnapshotIterator<>(numberOfEntriesInSnapshotData, snapshotData, stateSnapshotTransformer, minVersion);

		// if minVersion is set numberOfEntriesInSnapshotData will count for some irrelevant entries
		// todo: restore iterator.size encapsulation?
		sizeWriter.accept(minVersion.map(unused -> Iterators.size(snapshotIteratorSupplier.get())).orElse(numberOfEntriesInSnapshotData));
		Iterator<StateEntry<K, N, S>> snapshotIterator = snapshotIteratorSupplier.get();

		while (snapshotIterator.hasNext()) {
			entryWriter.accept((CopyOnWriteStateMap.StateMapEntry<K, N, S>) snapshotIterator.next());
		}
	}

	/**
	 * Iterator over state entries in a {@link CopyOnWriteStateMapSnapshot}.
	 */
	abstract static class SnapshotIterator<K, N, S> implements Iterator<StateEntry<K, N, S>> {

		int numberOfEntriesInSnapshotData;

		CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData;

		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> chainIterator;

		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> entryIterator;

		SnapshotIterator(
			int numberOfEntriesInSnapshotData,
			CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData,
			@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) {
			this.numberOfEntriesInSnapshotData = numberOfEntriesInSnapshotData;
			this.snapshotData = snapshotData;

			transform(stateSnapshotTransformer);
			this.chainIterator = getChainIterator();
			this.entryIterator = Collections.emptyIterator();
		}

		/**
		 * Return the number of state entries in this snapshot.
		 */
		abstract int size();

		/**
		 * Transform the state in the snapshot before iterating the state.
		 */
		abstract void transform(@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer);

		/**
		 * Return an iterator over the chains of entries in snapshotData.
		 */
		abstract Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getChainIterator();

		/**
		 * Return an iterator over the entries in the chain.
		 *
		 * @param stateMapEntry The head entry of the chain.
		 */
		abstract Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getEntryIterator(
			CopyOnWriteStateMap.StateMapEntry<K, N, S> stateMapEntry);

		@Override
		public boolean hasNext() {
			return entryIterator.hasNext() || chainIterator.hasNext();
		}

		@Override
		public CopyOnWriteStateMap.StateMapEntry<K, N, S> next() {
			if (entryIterator.hasNext()) {
				return entryIterator.next();
			}

			CopyOnWriteStateMap.StateMapEntry<K, N, S> stateMapEntry = chainIterator.next();
			entryIterator = getEntryIterator(stateMapEntry);
			return entryIterator.next();
		}
	}

	private static class NonTransformSnapshotIterator<K, N, S> implements Iterator<StateEntry<K, N, S>> {
		private final CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData;
		private final int minVersion;
		private CopyOnWriteStateMap.StateMapEntry<K, N, S> nextEntry;
		private int nextBucket = 0;

		NonTransformSnapshotIterator(CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData, Optional<Integer> minVersion) {
			this.snapshotData = snapshotData;
			this.minVersion = minVersion.orElse(-1);
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
			while (nextEntry != null && nextEntry.stateVersion < minVersion) { // todo: check entryVersion too?
				nextEntry = nextEntry.next;
			}
		}
	}

	/**
	 * Implementation of {@link SnapshotIterator} with a {@link StateSnapshotTransformer}.
	 */
	// todo: filter out empty (removing) states for non-incremental checkpoints
	static class TransformedSnapshotIterator<K, N, S> extends SnapshotIterator<K, N, S> {

		TransformedSnapshotIterator(
			int numberOfEntriesInSnapshotData,
			CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData,
			@Nonnull StateSnapshotTransformer<S> stateSnapshotTransformer,
			Optional<Integer> minVersion) {
			super(numberOfEntriesInSnapshotData, snapshotData, stateSnapshotTransformer);
			if (minVersion.isPresent()) {
				// todo: fixme
				throw new UnsupportedOperationException("TransformedSnapshotIterator doesn't support minVersion");
			}
		}

		/**
		 * Move the chains in snapshotData to the back of the array, and return the
		 * index of the first chain from the front.
		 */
		int moveChainsToBackOfArray() {
			int index = snapshotData.length - 1;
			// find the first null chain from the back
			while (index >= 0) {
				if (snapshotData[index] == null) {
					break;
				}
				index--;
			}

			int lastNullIndex = index;
			index--;
			// move the chains to the back
			while (index >= 0) {
				CopyOnWriteStateMap.StateMapEntry<K, N, S> entry = snapshotData[index];
				if (entry != null) {
					snapshotData[lastNullIndex] = entry;
					snapshotData[index] = null;
					lastNullIndex--;
				}
				index--;
			}
			// return the index of the first chain from the front
			return lastNullIndex + 1;
		}

		@Override
		void transform(@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) {
			Preconditions.checkNotNull(stateSnapshotTransformer);
			int indexOfFirstChain = moveChainsToBackOfArray();
			int count = 0;
			// reuse the snapshotData to transform and flatten the entries.
			for (int i = indexOfFirstChain; i < snapshotData.length; i++) {
				CopyOnWriteStateMap.StateMapEntry<K, N, S> entry = snapshotData[i];
				while (entry != null) {
					S transformedValue = stateSnapshotTransformer.filterOrTransform(entry.state);
					if (transformedValue != null) {
						CopyOnWriteStateMap.StateMapEntry<K, N, S> filteredEntry = entry;
						if (transformedValue != entry.state) {
							filteredEntry = new CopyOnWriteStateMap.StateMapEntry<>(entry, entry.entryVersion);
							filteredEntry.state = transformedValue;
						}
						snapshotData[count++] = filteredEntry;
					}
					entry = entry.next;
				}
			}
			numberOfEntriesInSnapshotData = count;
		}

		@Override
		public int size() {
			return numberOfEntriesInSnapshotData;
		}

		@Override
		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getChainIterator() {
			return Arrays.stream(snapshotData, 0, numberOfEntriesInSnapshotData).iterator();
		}

		@Override
		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getEntryIterator(
			CopyOnWriteStateMap.StateMapEntry<K, N, S> stateMapEntry) {
			return Collections.singleton(stateMapEntry).iterator();
		}
	}
}
