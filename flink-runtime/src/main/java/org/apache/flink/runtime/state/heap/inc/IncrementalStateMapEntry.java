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

import org.apache.flink.runtime.state.heap.CopyOnWriteStateMap;
import org.apache.flink.runtime.state.heap.StateMapEntry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One entry in the {@link CopyOnWriteStateMap}. This is a triplet of key, namespace, and state. Thereby, key and
 * namespace together serve as a composite key for the state. This class also contains some management meta data for
 * copy-on-write, a pointer to link other {@link IncrementalStateMapEntry}s to a list, and cached hash code.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
class IncrementalStateMapEntry<K, N, S> extends StateMapEntry<K, N, S> {

//	IncrementalStateMapEntry(IncrementalStateMapEntry<K, N, S> other, int entryVersion) {
//		this(other.key, other.namespace, other.state, other.journal.isStateExposed(), other.hash, other.next, entryVersion, other.stateVersion, other.stateJournalFactory);
//	}

	IncrementalStateMapEntry(
			@Nonnull K key,
			@Nonnull N namespace,
			@Nullable S state,
			boolean isStateExposed,
			int hash,
			@Nullable IncrementalStateMapEntry<K, N, S> next,
			int entryVersion,
			int stateVersion,
			StateJournalFactory<S, StateDiff<S>, StateJournal<S, StateDiff<S>>> stateJournalFactory) {
		super(
			key,
			namespace,
			state,
			isStateExposed,
			hash,
			next,
			entryVersion,
			stateVersion,
			stateJournalFactory);
	}

}
