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
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** @param <K> state object ID type (for {@link #distributedState}). */
@ThreadSafe
class TaskStateRegistryImpl<K> implements TaskStateRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStateRegistryImpl.class);

    /** The returned map should be modifiable. */
    @ThreadSafe
    public interface StateObjectIDExtractor<T> extends Function<StateObject, Map<T, StateObject>> {}

    private final TaskStateCleaner cleaner;
    private final StateObjectIDExtractor<K> keyExtractor;

    // Synchronization is to prevent issues inside backend registries, not just Map modification.
    // E.g. backends may share State Entries and otherwise, would update them concurrently.
    @GuardedBy("backendStateRegistries")
    private final Map<String, BackendStateRegistry<K>> backendStateRegistries = new HashMap<>();

    // Synchronization is just to prevent concurrent Map modification issues.
    @GuardedBy("distributedState")
    private final Set<K> distributedState;

    TaskStateRegistryImpl(TaskStateCleaner cleaner, StateObjectIDExtractor<K> keyExtractor) {
        this.cleaner = checkNotNull(cleaner);
        this.keyExtractor = checkNotNull(keyExtractor);
        this.distributedState = new HashSet<>();
    }

    @Override
    public void registerUsage(Set<String> backendIds, Collection<StateObject> states) {
        List<StateEntry<K>> entries = toStateEntries(states, backendIds.size());
        if (!entries.isEmpty()) {
            synchronized (backendStateRegistries) {
                for (String backendId : backendIds) {
                    withRegistry(backendId, registry -> registry.registerUsage(entries));
                }
            }
        }
    }

    private List<StateEntry<K>> toStateEntries(Collection<StateObject> states, int numBackends) {
        List<Map.Entry<K, StateObject>> unfiltered =
                states.stream()
                        .map(keyExtractor)
                        .flatMap(map -> map.entrySet().stream())
                        .collect(toList());
        synchronized (distributedState) {
            return unfiltered.stream()
                    .filter(e -> !distributedState.contains(e.getKey()))
                    .map(e -> new StateEntry<>(e.getKey(), e.getValue(), cleaner, numBackends))
                    .collect(toList());
        }
    }

    @Override
    public void unregisterUsage(String backendId, StateObject state) {
        Set<K> keys = keyExtractor.apply(state).keySet();
        if (!keys.isEmpty()) {
            withRegistry(backendId, registry -> registry.unregisterUsage(keys));
        }
    }

    @Override
    public void checkpointStarted(String backendId, long checkpointId, boolean managedExternally) {
        withRegistry(
                backendId, registry -> registry.checkpointStarted(checkpointId, managedExternally));
    }

    @Override
    public void checkpointPerformed(
            String backendId,
            StateObject state,
            long checkpointId,
            boolean inferUnusedFromPrevious) {
        withRegistry(
                backendId,
                registry -> registry.snapshotSent(checkpointId, state, inferUnusedFromPrevious));
    }

    @Override
    public void checkpointSubsumed(String backendId, long checkpointId) {
        withRegistry(backendId, registry -> registry.checkpointSubsumed(checkpointId));
    }

    @Override
    public void checkpointAborted(String backendId, long checkpointId) {
        withRegistry(backendId, registry -> registry.checkpointAborted(checkpointId));
    }

    @Override
    public void discardAll() {
        synchronized (backendStateRegistries) {
            backendStateRegistries.values().forEach(BackendStateRegistry::discardAll);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Close");
        synchronized (backendStateRegistries) {
            backendStateRegistries.values().forEach(BackendStateRegistry::close);
            backendStateRegistries.clear();
        }
        synchronized (distributedState) {
            distributedState.clear();
        }
        cleaner.close();
    }

    // todo: move to interface? (when rebased?)
    public void addDistributedState(Set<K> distributedState) {
        synchronized (this.distributedState) {
            this.distributedState.addAll(distributedState);
        }
    }

    private <E extends RuntimeException> void withRegistry(
            String backendId, ThrowingConsumer<BackendStateRegistry<K>, E> action) throws E {
        checkArgument(backendId != null && !backendId.isEmpty());
        synchronized (backendStateRegistries) {
            BackendStateRegistry<K> backendStateRegistry =
                    backendStateRegistries.computeIfAbsent(
                            backendId, ign -> new BackendStateRegistry<>(backendId, keyExtractor));
            action.accept(backendStateRegistry);
        }
    }
}
