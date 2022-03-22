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

package org.apache.flink.state.hashmap;

import org.apache.flink.runtime.state.CheckpointBoundKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

class StateHandleHelper {
    private final UUID backendIdentifier;
    private final KeyGroupRange keyGroupRange;

    StateHandleHelper(UUID backendIdentifier, KeyGroupRange keyGroupRange) {
        this.backendIdentifier = backendIdentifier;
        this.keyGroupRange = keyGroupRange;
    }

    /**
     * Transform the initial state after restore into a {@link SnapshotResult} that can be used as a
     * base for the next (incremental) checkpoints.
     */
    public SnapshotResult<KeyedStateHandle> init(Collection<KeyedStateHandle> stateHandles) {
        Map<StateHandleID, StreamStateHandle> sharedState = new HashMap<>();
        long checkpointId = 0L;
        // multiple handles in case of down-scaling
        for (KeyedStateHandle handle : stateHandles) {
            // todo: on migration, initial checkpoint can be subsumed (in CLAIM mode) and its state
            // discarded (similar to FLINK-25872)
            putStateBase(handle, sharedState);
            checkpointId =
                    Math.max(
                            checkpointId,
                            handle instanceof CheckpointBoundKeyedStateHandle
                                    ? ((CheckpointBoundKeyedStateHandle) handle).getCheckpointId()
                                    : checkpointId);
        }
        return toSnapshotResult(sharedState, checkpointId, 0);
    }

    /** Package old and new state snapshots into a single {@link SnapshotResult}. */
    public SnapshotResult<KeyedStateHandle> combine(
            SnapshotResult<KeyedStateHandle> stateBase,
            SnapshotResult<KeyedStateHandle> stateDelta,
            long checkpointId) {
        // todo: local recovery
        Map<StateHandleID, StreamStateHandle> jmState = new HashMap<>();
        putStateBase(stateBase.getJobManagerOwnedSnapshot(), jmState);
        putStateDelta(stateDelta.getJobManagerOwnedSnapshot(), jmState, checkpointId);
        return toSnapshotResult(jmState, checkpointId, stateDelta.getStateSize());
    }

    private void putStateBase(KeyedStateHandle base, Map<StateHandleID, StreamStateHandle> state) {
        if (base instanceof IncrementalKeyedStateHandle) {
            state.putAll(((IncrementalKeyedStateHandle) base).getSharedStateHandles());
        } else if (base instanceof KeyGroupsStateHandle) {
            state.put(base.getStateHandleId(), (KeyGroupsStateHandle) base);
        } else if (base != null) {
            throw new RuntimeException("Unexpected state type: " + base.getClass());
        }
    }

    private void putStateDelta(
            KeyedStateHandle delta,
            Map<StateHandleID, StreamStateHandle> state,
            long checkpointId) {
        if (delta != null) {
            state.put(buildStateID(checkpointId, delta), (KeyGroupsStateHandle) delta);
        }
    }

    private SnapshotResult<KeyedStateHandle> toSnapshotResult(
            Map<StateHandleID, StreamStateHandle> sharedState,
            long checkpointId,
            long newStateSize) {
        // todo: local recovery
        return SnapshotResult.of(
                new IncrementalRemoteKeyedStateHandle(
                        backendIdentifier,
                        keyGroupRange,
                        checkpointId,
                        sharedState,
                        emptyMap(), // not used
                        new ByteStreamStateHandle("empty", new byte[0]), // not used
                        newStateSize));
    }

    private List<KeyedStateHandle> flatMap(IncrementalRemoteKeyedStateHandle keyedStateHandle) {
        return keyedStateHandle.getSharedState().entrySet().stream()
                .sorted(STATE_COMPARATOR)
                .map(Map.Entry::getValue)
                .map(handle -> (KeyedStateHandle) handle)
                .collect(toList());
    }

    Collection<KeyedStateHandle> flattenState(Collection<KeyedStateHandle> stateHandles) {
        Collection<KeyedStateHandle> result = new ArrayList<>();
        for (KeyedStateHandle keyedStateHandle : stateHandles) {
            if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                result.addAll(flatMap((IncrementalRemoteKeyedStateHandle) keyedStateHandle));
            } else {
                result.add(keyedStateHandle);
            }
        }
        return result;
    }

    private StateHandleID buildStateID(long checkpointId, KeyedStateHandle newKeyedState) {
        // handle down-scaling, allow sort by checkpoint ID --see comparator below
        return new StateHandleID(
                String.format(
                        "%d-%s", checkpointId, newKeyedState.getStateHandleId().getKeyString()));
    }

    /**
     * Sorts state entries according to:
     *
     * <ol>
     *   <li>{@link IncrementalKeyedStateHandle}s follow the full ones
     *   <li>checkpoint ID order (for {@link CheckpointBoundKeyedStateHandle}s)
     *   <li>{@link StateHandleID#getKeyString()} order
     * </ol>
     */
    private static final Comparator<Map.Entry<StateHandleID, StreamStateHandle>> STATE_COMPARATOR =
            ((Comparator<Map.Entry<StateHandleID, StreamStateHandle>>)
                            (e1, e2) -> {
                                if (isFull(e1.getValue())) {
                                    return -1;
                                } else if (isFull(e2.getValue())) {
                                    return 1;
                                } else {
                                    return 0;
                                }
                            })
                    .thenComparingLong(StateHandleHelper::extractCheckpointID)
                    .thenComparing(entry -> entry.getKey().getKeyString());

    private static boolean isFull(StreamStateHandle value) {
        return !(value instanceof IncrementalKeyedStateHandle);
    }

    private static long extractCheckpointID(Map.Entry<StateHandleID, StreamStateHandle> entry) {
        StreamStateHandle value = entry.getValue();
        if (value instanceof CheckpointBoundKeyedStateHandle) {
            return ((CheckpointBoundKeyedStateHandle) value).getCheckpointId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    public SnapshotResult<KeyedStateHandle> rebuildWithPlaceHolders(
            SnapshotResult<KeyedStateHandle> stateSnapshot) {
        IncrementalKeyedStateHandle jmState =
                (IncrementalKeyedStateHandle) stateSnapshot.getJobManagerOwnedSnapshot();
        Map<StateHandleID, StreamStateHandle> sharedStateHandles = new HashMap<>();
        for (Map.Entry<StateHandleID, StreamStateHandle> e :
                jmState.getSharedStateHandles().entrySet()) {
            sharedStateHandles.put(
                    e.getKey(), new PlaceholderStreamStateHandle(e.getValue().getStateSize()));
        }
        return toSnapshotResult(sharedStateHandles, jmState.getCheckpointId(), 0);
    }
}
