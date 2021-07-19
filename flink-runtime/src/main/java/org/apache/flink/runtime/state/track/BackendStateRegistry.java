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

package org.apache.flink.runtime.state.track;

import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.track.StateEntry.StateEntryBackendUsage;
import org.apache.flink.runtime.state.track.TaskStateRegistryImpl.StateObjectIDExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.flink.runtime.state.track.StateEntry.StateEntryBackendUsage.USED_IN_CHECKPOINTS;
import static org.apache.flink.runtime.state.track.StateEntry.StateEntryBackendUsage.USED_IN_SAVEPOINTS;
import static org.apache.flink.util.Preconditions.checkState;

// todo: javadoc for algorithm, edge cases, thread sync, lifecycle; mention alternatives
@NotThreadSafe
class BackendStateRegistry<K> {
    private final Logger LOG = LoggerFactory.getLogger(BackendStateRegistry.class);

    private final String backendId;
    private final StateObjectIDExtractor<K> keyExtractor;

    private final NavigableSet<Long> pendingCheckpoints = new TreeSet<>();
    private final NavigableSet<Long> pendingSavepoints = new TreeSet<>();
    @Nullable private Set<K> lastSnapshot = null;

    private final Map<K, StateEntry<K>> inActiveUse = new HashMap<>();
    private final Map<K, StateEntry<K>> inUseBySavepoints = new HashMap<>();
    private final NavigableMap<Long, List<StateEntry<K>>> inUseByCheckpoints = new TreeMap<>();

    public BackendStateRegistry(String backendId, StateObjectIDExtractor<K> keyExtractor) {
        this.keyExtractor = keyExtractor;
        this.backendId = backendId;
    }

    public void registerUsage(Collection<StateEntry<K>> entries) {
        LOG.debug("State used, backend: {}, state: {}", backendId, entries);
        entries.forEach(e -> inActiveUse.put(e.getKey(), e));
    }

    public void unregisterUsage(Set<K> keys) {
        for (K key : keys) {
            StateEntry<K> entry = inActiveUse.remove(key);
            if (entry == null) {
                continue;
            }
            switch (entry.markCheckpointUsage(
                    backendId,
                    new TreeSet<>(pendingSavepoints),
                    new TreeSet<>(pendingCheckpoints))) {
                case USED_IN_SAVEPOINTS:
                    LOG.debug(
                            "State entry not used but {} pending savepoints exist, backend: {}, state: {}",
                            pendingSavepoints.size(),
                            backendId,
                            keys);
                    inUseBySavepoints.put(entry.getKey(), entry);
                    break;
                case USED_IN_CHECKPOINTS:
                    LOG.debug(
                            "State entry not used but {} pending checkpoints exist, backend: {}, state: {}",
                            pendingCheckpoints.size(),
                            backendId,
                            keys);
                    inUseByCheckpoints
                            .computeIfAbsent(pendingCheckpoints.last(), ign -> new ArrayList<>())
                            .add(entry);
                    break;
                case NOT_USED:
                    LOG.debug("State entry not used, backend: {}, state: {}", backendId, keys);
                    break;
                default:
                    throw new RuntimeException("Unknown StateEntryBackendUse type");
            }
        }
    }

    public void checkpointStarted(long checkpointId, boolean managedExternally) {
        LOG.debug(
                "Checkpoint started, backend: {}, checkpoint: {}, managedExternally: {}",
                backendId,
                checkpointId,
                managedExternally);
        checkState(
                (managedExternally ? pendingSavepoints : pendingCheckpoints).add(checkpointId),
                "Backend %s has already started checkpoint %s",
                backendId,
                checkpointId);
    }

    public void snapshotSent(
            long checkpointId, StateObject state, boolean inferUnusedFromPrevious) {
        Set<K> newStateKeys = keyExtractor.apply(state).keySet();
        LOG.debug(
                "Checkpoint performed, backend: {}, checkpoint: {}, inferUnusedFromPrevious: {}, state objects: {}",
                backendId,
                checkpointId,
                inferUnusedFromPrevious,
                newStateKeys);

        if (pendingSavepoints.remove(checkpointId)) {
            for (K k : newStateKeys) {
                if (inActiveUse.containsKey(k)) {
                    inActiveUse.remove(k).markUsedExternally(backendId, checkpointId);
                } else if (inUseBySavepoints.containsKey(k)) {
                    inUseBySavepoints.remove(k).markUsedExternally(backendId, checkpointId);
                } else {
                    throw new NoSuchElementException();
                }
            }
        } else {
            checkState(
                    pendingCheckpoints.contains(checkpointId),
                    "Checkpoint %s not known to %s",
                    backendId,
                    checkpointId);
            if (inferUnusedFromPrevious && lastSnapshot != null) {
                lastSnapshot.removeAll(newStateKeys);
                unregisterUsage(lastSnapshot);
            }
            lastSnapshot = newStateKeys;
        }
    }

    public void checkpointSubsumed(long checkpointId) {
        LOG.debug("Checkpoint subsumed, backend: {}, checkpoint: {}", backendId, checkpointId);
        pendingCheckpoints.headSet(checkpointId, true).clear();
        Collection<List<StateEntry<K>>> subsumed =
                inUseByCheckpoints.headMap(checkpointId, true).values();
        subsumed.forEach(entries -> entries.forEach(StateEntry::markUnused));
        subsumed.clear();
    }

    public void checkpointAborted(long checkpointId) {
        LOG.debug("Checkpoint aborted, backend: {}, checkpoint: {}", backendId, checkpointId);
        if (!pendingSavepoints.remove(checkpointId)) {
            pendingCheckpoints.remove(checkpointId);
        }
        Iterator<StateEntry<K>> it = inUseBySavepoints.values().iterator();
        while (it.hasNext()) {
            StateEntry<K> entry = it.next();
            StateEntryBackendUsage usage = entry.checkpointAborted(backendId, checkpointId);
            if (usage != USED_IN_SAVEPOINTS) {
                it.remove();
            }
            if (usage == USED_IN_CHECKPOINTS) {
                inUseByCheckpoints
                        .computeIfAbsent(
                                entry.getRemainingCheckpoints(backendId).last(),
                                ign -> new ArrayList<>())
                        .add(entry);
            }
        }
    }

    public void discardAll() {
        LOG.debug("Discard all, backendId: {}", backendId);
        inActiveUse.values().forEach(StateEntry::markUnused);
        inUseBySavepoints.values().forEach(StateEntry::markUnused);
        inUseByCheckpoints.values().forEach(l -> l.forEach(StateEntry::markUnused));
        inActiveUse.clear();
        inUseBySavepoints.clear();
        inUseByCheckpoints.clear();
    }

    public void close() {
        LOG.debug("Close, backendId: {}", backendId);
    }
}
