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
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMap;
import org.apache.flink.runtime.state.heap.StateMapEntry;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

class IncrementalCopyOnWriteStateMap<K, N, S> extends CopyOnWriteStateMap<K, N, S> {

	private final StateJournalFactory<S, StateDiff<S>, StateJournal<S, StateDiff<S>>> stateJournalFactory;

	private final RemovalLog<K, N> removalLog = new RemovalLogImpl<>();

	IncrementalCopyOnWriteStateMap(TypeSerializer<S> stateSerializer, StateJournalFactory<S, StateDiff<S>, StateJournal<S, StateDiff<S>>> stateJournalFactory) {
		super(stateSerializer);
		this.stateJournalFactory = stateJournalFactory;
	}

	public Collection<Map<K, Set<N>>> getRemovedKeys(int version) {
		return removalLog.collect(version);
	}

	protected StateMapEntry<K, N, S> putEntry(K key, N namespace) {
		removalLog.added(key, namespace);
		return super.putEntry(key, namespace);
	}

	protected S removeEntry(K key, N namespace) {
		removalLog.removed(key, namespace);
		return super.removeEntry(key, namespace);
	}

	public Collection<Map<K, Set<N>>> snapshotRemovedKeys() {
		removalLog.truncate();
		removalLog.startNewVersion(stateMapVersion);
		return getRemovedKeys(stateMapVersion);
	}

	@Override
	public void confirmSnapshot(int version) {
		removalLog.confirmed(version);
	}

	public IncrementalCopyOnWriteStateMapSnapshot<K, N, S> incrementalStateSnapshot() {
		return new IncrementalCopyOnWriteStateMapSnapshot<>(this);
	}

	@Override
	protected void setStateBeforeRead(StateMapEntry<K, N, S> e) {
		(asIncremental(e)).setState(copyState(e), stateMapVersion, true, false);
	}

	@Override
	protected void setStateOnWrite(StateMapEntry<K, N, S> e, S value) {
		(asIncremental(e)).setState(value, stateMapVersion, false, true);
	}

	@Override
	protected StateMapEntry<K, N, S> createEntry(N namespace, K key, int hash, StateMapEntry<K, N, S> next) {
		return new IncrementalStateMapEntry<>(
			key,
			namespace,
			false,
			hash,
			next,
			stateMapVersion,
			stateMapVersion,
			stateJournalFactory);
	}

	@Override
	protected StateMapEntry<K, N, S> copyEntry(StateMapEntry<K, N, S> e) {
		return new IncrementalStateMapEntry<>(asIncremental(e), stateMapVersion);
	}

	@Override
	protected void beforeFirstReadAfterSnapshot(StateMapEntry<K, N, S> e) {
		super.beforeFirstReadAfterSnapshot(e);
		asIncremental(e).setState(
			e.getState(), // the state itself is not changing
			stateMapVersion, // only need to roll-over the journal (to snapshot each change exactly once)
			true, // not expecting any replace
			false // state can't be exposed until put is called
		);
	}

	private IncrementalStateMapEntry<K, N, S> asIncremental(StateMapEntry<K, N, S> e) {
		return (IncrementalStateMapEntry<K, N, S>) e;
	}
}
