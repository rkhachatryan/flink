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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import static java.util.Collections.emptyNavigableSet;
import static org.apache.flink.runtime.state.track.StateEntry.StateEntryBackendUsage.NOT_USED;
import static org.apache.flink.runtime.state.track.StateEntry.StateEntryBackendUsage.USED_IN_CHECKPOINTS;
import static org.apache.flink.runtime.state.track.StateEntry.StateEntryBackendUsage.USED_IN_SAVEPOINTS;
import static org.apache.flink.util.Preconditions.checkState;

// todo: javadoc lifecycle
@NotThreadSafe
class StateEntry<K> {
    private static final Logger LOG = LoggerFactory.getLogger(StateEntry.class);

    private final K key;
    private final StateObject state;
    private final TaskStateCleaner cleaner;

    private final Map<String, NavigableSet<Long>> pendingCheckpointsByBackend = new HashMap<>();
    private final Map<String, NavigableSet<Long>> pendingSavepointsByBackend = new HashMap<>();

    private int backendCount;
    private boolean discarded = false;
    private boolean usedExternally = false;

    StateEntry(K key, StateObject state, TaskStateCleaner cleaner, int backendCount) {
        this.key = key;
        this.state = state;
        this.cleaner = cleaner;
        this.backendCount = backendCount;
    }

    public StateEntryBackendUsage markCheckpointUsage(
            String backendId, NavigableSet<Long> savepoints, NavigableSet<Long> checkpoints) {
        LOG.trace(
                "Update state entry usage, backendId: {}, savepoints: {}, checkpoints: {}",
                backendId,
                savepoints,
                checkpoints);
        checkState(!discarded && backendCount > 0);
        put(savepoints, pendingSavepointsByBackend, backendId);
        put(checkpoints, pendingCheckpointsByBackend, backendId);
        return onCheckpointsStatusUpdate(savepoints, checkpoints);
    }

    public void markUnused() {
        checkState(!discarded && backendCount > 0);
        if (--backendCount <= 0) {
            discard();
        }
    }

    public void markUsedExternally(String backendId, long checkpointId) {
        NavigableSet<Long> savepoints = pendingSavepointsByBackend.get(backendId);
        checkState(
                !discarded
                        && backendCount > 0
                        && (savepoints == null || savepoints.contains(checkpointId)),
                "State entry %s can't be used externally by %s for checkpoint %s",
                this,
                backendId,
                checkpointId);
        usedExternally = true;
    }

    private void discard() {
        if (!discarded) {
            discarded = true;
            if (!usedExternally) {
                cleaner.discardAsync(state);
            }
        }
    }

    public NavigableSet<Long> getRemainingCheckpoints(String backendId) {
        return pendingCheckpointsByBackend.getOrDefault(backendId, emptyNavigableSet());
    }

    public NavigableSet<Long> getRemainingSavepoints(String backendId) {
        return pendingSavepointsByBackend.getOrDefault(backendId, emptyNavigableSet());
    }

    private static void put(
            NavigableSet<Long> savepoints,
            Map<String, NavigableSet<Long>> pendingSavepointsByBackend,
            String backendId) {
        checkState(pendingSavepointsByBackend.put(backendId, savepoints) == null);
    }

    public StateEntryBackendUsage checkpointAborted(String backendId, long checkpointId) {
        NavigableSet<Long> remainingSavepoints = getRemainingSavepoints(backendId);
        remainingSavepoints.remove(checkpointId);
        NavigableSet<Long> remainingCheckpoints = getRemainingCheckpoints(backendId);
        remainingCheckpoints.remove(checkpointId);
        return onCheckpointsStatusUpdate(remainingSavepoints, remainingCheckpoints);
    }

    private StateEntryBackendUsage onCheckpointsStatusUpdate(
            Set<Long> savepoints, Set<Long> checkpoints) {
        if (!savepoints.isEmpty()) {
            return USED_IN_SAVEPOINTS;
        } else if (!checkpoints.isEmpty()) {
            return USED_IN_CHECKPOINTS;
        } else {
            markUnused();
            return NOT_USED;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "key=%s, state=%s, discarded=%s, backendCount=%d",
                key, state, discarded, backendCount);
    }

    public K getKey() {
        return key;
    }

    public enum StateEntryBackendUsage {
        USED_IN_SAVEPOINTS,
        USED_IN_CHECKPOINTS,
        NOT_USED
    }
}
