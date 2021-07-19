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

// todo: consider renaming package
package org.apache.flink.runtime.state.track;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StateObject;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;

// todo: update javadoc:
//  serializability for single backend, across backends
//  atomicity for regState
// order of: regState and start, unreg and start
/**
 * A component responsible for managing state that is not shared across different TMs (i.e. not
 * managed by JM). Managing state here means {@link StateObject#discardState() deletion} once not
 * used.
 *
 * <p>Each {@link StateObject} can be marked as used or not used by a backend or a checkpoint. It is
 * discarded once neither is using it.
 *
 * <p>The purpose of marking state objects as used separately by backend and by checkpoint is to
 * prevent its premature deletion in case of incremental checkpoints (where a checkpoint can be
 * subsumed but the state could still be used).
 *
 * <h1>State sharing inside TM</h1>
 *
 * <p>Inside each TM, the registry <strong>must</strong> be shared across state backends on the same
 * level as their state is, or higher. For example, if all state backends of operators of the same
 * job in TM use the same file on DFS then they <strong>must</strong> share the same registry. OTH,
 * if nothing is shared then the Registry can be per-backend (or shared).
 *
 * <h1>State sharing across TMs</h1>
 *
 * If a rescaling operation (or recovery with state sharing inside TM) results in some state being
 * shared by multiple TMs (or TaskStateRegistries), such relevant shared states must be communicated
 * to the registry so that it can ignore them. Implementations <strong>must</strong> ignore such
 * shared states.
 *
 * <h1>State identity</h1>
 *
 * For the purpose of matching state objects from the different calls (and from previous runs),
 * state can be identified by {@link StateObjectID}. A collection of such IDs can be obtained from
 * the {@link StateObject} by recursively traversing it. This is implementation specific; however,
 * it must be consistent across attempts, registries and backends.
 *
 * <h1>Thread safety</h1>
 *
 * Thread safety depends on the implementation. An implementation shared across different tasks (in
 * a single TM) <strong>must</strong> be thread-safe.
 *
 * <h1>Tracking of state associated with a checkpoint</h1>
 *
 * <h1>Discarding the state</h1>
 *
 * Production implementations might choose to discard the state asynchronously; however, they might
 * chose to block the caller if they don't keep up.
 */
@Internal
public interface TaskStateRegistry extends AutoCloseable {

    /**
     * Mark given state object as used by the given state backend (idempotent operation). Should be
     * called upon initial creation of the object (e.g. upload to DFS).
     */
    void registerUsage(Set<String> backendIds, Collection<StateObject> states);

    /**
     * Mark state object as not used anymore by the given backend; discard if not used by any other
     * backend or checkpoint. When using incremental checkpoints, it should be called upon
     * materialization; otherwise, on checkpoint subsumption (in addition to {@link
     * #checkpointSubsumed(String, long)}. The method does nothing if the state is not marked as
     * used.
     */
    void unregisterUsage(String backendId, StateObject state);

    /**
     * todo: javadoc
     *
     * @param backendId
     * @param checkpointId
     * @param managedExternally true for externalized checkpoints and savepoints. Such checkpoint
     *     state will only be discarded if the checkpoint is {@link #checkpointAborted(String,
     *     long)} (and not used by other checkpoints/backends).
     */
    void checkpointStarted(String backendId, long checkpointId, boolean managedExternally);

    /**
     * Mark the given state as being used in a given backend snapshot for the given checkpoint.
     *
     * <p>See {@link #checkpointPerformed(String, StateObject, long, boolean)}
     *
     * @throws NoSuchElementException if the state is not registered
     * @throws IllegalStateException if the given checkpoint was already performed
     */
    default void checkpointPerformed(String backendId, StateObject state, long checkpointId)
            throws NoSuchElementException, IllegalStateException {
        checkpointPerformed(backendId, state, checkpointId, false);
    }

    /**
     * Mark the given state as being used in a given backend snapshot for the given checkpoint.
     *
     * @param inferUnusedFromPrevious if true then any state objects from the previous checkpoint
     *     that are <strong>not</strong> used this checkpoint will be marked as unused (and
     *     potentially discarded). This is equivalent to manually {@link #unregisterUsage(String,
     *     StateObject) marking such states as unused}. This will only take effect if the previous
     *     checkpointId differs exactly by 1L from the current one (a gap might mean that some older
     *     checkpoint may be completed later).
     * @throws NoSuchElementException if the state is not registered
     * @throws IllegalStateException if the given checkpoint was already performed
     */
    void checkpointPerformed(
            String backendId, StateObject state, long checkpointId, boolean inferUnusedFromPrevious)
            throws NoSuchElementException, IllegalStateException;
    /**
     * Un-mark any states previously {@link #checkpointPerformed(String, StateObject, long, boolean)
     * marked} as used by given backend in the given checkpoint. Doesn't affect any other
     * checkpoints. All state objects that become unused are discarded (even if they are managed
     * externally).
     */
    void checkpointAborted(String backendId, long checkpointId);

    /**
     * Un-mark any states previously {@link #checkpointPerformed(String, StateObject, long, boolean)
     * marked} as used by given backend in the given checkpoint. All earlier checkpoints of
     * <strong>this backend</strong> are also considered as subsumed. All state objects that become
     * unused by any backend or checkpoint anymore are discarded (unless managed externally).
     */
    void checkpointSubsumed(String backendId, long checkpointId);

    /**
     * Discard all states (except for those managed externally). Can be used in case of job
     * cancellation.
     */
    void discardAll();

    TaskStateRegistry NO_OP =
            new TaskStateRegistry() {

                @Override
                public void registerUsage(Set<String> backendId, Collection<StateObject> state) {}

                @Override
                public void unregisterUsage(String backendId, StateObject state) {}

                @Override
                public void checkpointStarted(
                        String backendId, long checkpointId, boolean managedExternally) {}

                @Override
                public void checkpointPerformed(
                        String backendId,
                        StateObject state,
                        long checkpointId,
                        boolean inferUnusedFromPrevious)
                        throws NoSuchElementException, IllegalStateException {}

                @Override
                public void checkpointAborted(String backendId, long checkpointId) {}

                @Override
                public void checkpointSubsumed(String backendId, long checkpointId) {}

                @Override
                public void discardAll() {}

                @Override
                public void close() {}
            };
}
