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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMap;
import org.apache.flink.runtime.state.heap.StateMapEntry;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Incremental {@link CopyOnWriteStateMap}.
 */
@Internal
public class IncrementalCopyOnWriteStateMap<K, N, S> extends CopyOnWriteStateMap<K, N, S> {

	private final RemovalLog<K, N> removalLog = new RemovalLogImpl<>(); // todo: noop for non-inc; move to stateJournalFactory?

	IncrementalCopyOnWriteStateMap(
			TypeSerializer<S> stateSerializer,
			StateJournalFactory<S, StateDiff<S>, StateJournal<S, StateDiff<S>>> stateJournalFactory) {
		super(stateSerializer, stateJournalFactory);
	}

	public Collection<Map<K, Set<N>>> getRemovedKeys(int version) {
		return removalLog.collect(version);
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

	@Override
	protected StateMapEntry<K, N, S> putEntry(K key, N namespace) {
		removalLog.added(key, namespace);
		return super.putEntry(key, namespace);
	}

	@Override
	protected S removeEntry(K key, N namespace) {
		removalLog.removed(key, namespace);
		return super.removeEntry(key, namespace);
	}

	public IncrementalStateMapSnapshot<K, N, S> incrementalStateSnapshot() {
		return new IncrementalStateMapSnapshot<>(this);
	}

}
