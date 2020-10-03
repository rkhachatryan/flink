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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.heap.inc.StateDiff;
import org.apache.flink.runtime.state.heap.inc.StateDiffSerializer;
import org.apache.flink.runtime.state.heap.inc.StateJournal;
import org.apache.flink.runtime.state.heap.inc.StateJournalFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * One entry in the {@link CopyOnWriteStateMap}. This is a triplet of key, namespace, and state. Thereby, key and
 * namespace together serve as a composite key for the state. This class also contains some management meta data for
 * copy-on-write, a pointer to link other {@link StateMapEntry}s to a list, and cached hash code.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
@Internal
public class StateMapEntry<K, N, S> implements StateEntry<K, N, S> {

	/**
	 * The key. Assumed to be immumap and not null.
	 */
	@Nonnull
	final K key;

	/**
	 * The namespace. Assumed to be immumap and not null.
	 */
	@Nonnull
	final N namespace;

	StateJournal<S, StateDiff<S>> journal;

	/**
	 * The state. This is not final to allow exchanging the object for copy-on-write. Can be null.
	 */
	@Nullable
	S state;

	/**
	 * Link to another {@link StateMapEntry}. This is used to resolve collisions in the
	 * {@link CopyOnWriteStateMap} through chaining.
	 */
	@Nullable
	StateMapEntry<K, N, S> next;

	final StateJournalFactory<S, StateDiff<S>, StateJournal<S, StateDiff<S>>> stateJournalFactory;

	/**
	 * The version of this {@link StateMapEntry}. This is meta data for copy-on-write of the map structure.
	 */
	int entryVersion;

	/**
	 * The version of the state object in this entry. This is meta data for copy-on-write of the state object itself.
	 */
	int stateVersion;

	/**
	 * The computed secondary hash for the composite of key and namespace.
	 */
	final int hash;

	public StateMapEntry(StateMapEntry<K, N, S> other, int entryVersion) {
		this(other.key, other.namespace, other.state, other.journal.isStateExposed(), other.hash, other.next, entryVersion, other.stateVersion, other.stateJournalFactory);
	}

	public StateMapEntry(
			@Nonnull K key,
			@Nonnull N namespace,
			@Nullable S state,
			boolean isStateExposed,
			int hash,
			@Nullable StateMapEntry<K, N, S> next,
			int entryVersion,
			int stateVersion,
			StateJournalFactory<S, StateDiff<S>, StateJournal<S, StateDiff<S>>> stateJournalFactory) {
		this.key = key;
		this.namespace = namespace;
		this.hash = hash;
		this.next = next;
		this.entryVersion = entryVersion;
		this.stateVersion = stateVersion;
		this.stateJournalFactory = stateJournalFactory;
		this.journal = this.stateJournalFactory.createJournal(state, false, isStateExposed);
		this.state = this.journal.getJournaledState();
	}

	public void setState(@Nullable S value, int mapVersion, boolean isIncremental, boolean isValueExposed) {
		this.journal = stateJournalFactory.createJournal(value, !isIncremental, isValueExposed);
		this.state = journal.getJournaledState();
		this.stateVersion = mapVersion;
	}

	@Nonnull
	@Override
	public K getKey() {
		return key;
	}

	@Nonnull
	@Override
	public N getNamespace() {
		return namespace;
	}

	@Nullable
	@Override
	public S getState() {
		return state;
	}

	@Override
	public final boolean equals(Object o) {
		if (!(o instanceof StateMapEntry)) {
			return false;
		}

		StateEntry<?, ?, ?> e = (StateEntry<?, ?, ?>) o;
		return e.getKey().equals(key)
			&& e.getNamespace().equals(namespace)
			&& Objects.equals(e.getState(), state);
	}

	@Override
	public final int hashCode() {
		return (key.hashCode() ^ namespace.hashCode()) ^ Objects.hashCode(state);
	}

	@Override
	public final String toString() {
		return "(" + key + "|" + namespace + ")=" + state;
	}

	void writeStateDiff(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, StateDiffSerializer<S, StateDiff<S>> diffSerializer, DataOutputView dov) throws IOException {
		namespaceSerializer.serialize(this.getNamespace(), dov);
		keySerializer.serialize(this.getKey(), dov);
		diffSerializer.serialize(journal.getDiff(), dov);
	}
}
