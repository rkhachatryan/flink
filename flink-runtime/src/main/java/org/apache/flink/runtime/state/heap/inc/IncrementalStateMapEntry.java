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
import org.apache.flink.runtime.state.heap.StateMapEntry;

import javax.annotation.Nullable;

import java.io.IOException;

class IncrementalStateMapEntry<K, N, S> extends StateMapEntry<K, N, S> {

	private final StateJournalFactory<S, StateDiff<S>, StateJournal<S, StateDiff<S>>> stateJournalFactory;
	private StateJournal<S, StateDiff<S>> journal;

	IncrementalStateMapEntry(IncrementalStateMapEntry<K, N, S> other, int entryVersion) {
		super(other, entryVersion);
		this.stateJournalFactory = other.stateJournalFactory;
		this.journal = this.stateJournalFactory.createJournal(state, other.journal.wasCleared(), other.journal.isStateExposed());
		this.state = journal.getJournaledState();
	}

	IncrementalStateMapEntry(
			K key,
			N namespace,
			boolean isStateExposed,
			int hash,
			StateMapEntry<K, N, S> next,
			int entryVersion,
			int stateVersion,
			StateJournalFactory<S, StateDiff<S>, StateJournal<S, StateDiff<S>>> stateJournalFactory) {
		super(key, namespace, null, hash, next, entryVersion, stateVersion);
		this.stateJournalFactory = stateJournalFactory;
		this.journal = this.stateJournalFactory.createJournal(null, true, isStateExposed);
		this.state = journal.getJournaledState();
	}

	public void setState(@Nullable S value, int mapVersion, boolean isIncremental, boolean isValueExposed) {
		this.journal = stateJournalFactory.createJournal(value, !isIncremental, isValueExposed);
		this.state = journal.getJournaledState();
		this.stateVersion = mapVersion;
	}

	void writeStateDiff(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, StateDiffSerializer<S, StateDiff<S>> diffSerializer, DataOutputView dov) throws IOException {
		keySerializer.serialize(this.getKey(), dov);
		namespaceSerializer.serialize(this.getNamespace(), dov);
		diffSerializer.serialize(journal.getDiff(), dov);
	}

	public IncrementalStateMapEntry<K, N, S> next() {
		return (IncrementalStateMapEntry<K, N, S>) next; // todo: remove cast
	}

	public int getEntryVersion() {
		return super.entryVersion;
	}

	public int getStateVersion() {
		return super.stateVersion;
	}
}
