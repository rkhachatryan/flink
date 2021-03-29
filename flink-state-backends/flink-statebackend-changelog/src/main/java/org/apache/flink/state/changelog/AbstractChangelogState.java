/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.state.changelog.StateChangeLogReader.ChangeApplier;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Base class for changelog state wrappers of state objects.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <V> The type of values kept internally in state without changelog wrapper
 * @param <S> Type of originally wrapped state object
 */
abstract class AbstractChangelogState<K, N, V, S extends InternalKvState<K, N, V>>
        implements InternalKvState<K, N, V> {
    protected final S delegatedState;
    protected N currentNamespace;
    protected StateChangeLogger<V, N> changeLogger;
    protected final short stateId; // todo: drop
    protected final InternalKeyContext<K> keyContext; // todo: drop

    AbstractChangelogState(
            S state,
            StateChangeLogger<V, N> changeLogger,
            InternalKeyContext<K> keyContext,
            short stateId) {
        this.keyContext = keyContext;
        checkArgument(!(state instanceof AbstractChangelogState));
        checkArgument(stateId >= 0);
        this.stateId = stateId;
        this.delegatedState = state;
        this.changeLogger = changeLogger;
    }

    public S getDelegatedState() {
        return delegatedState;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegatedState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return delegatedState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return delegatedState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        currentNamespace = namespace;
        delegatedState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        return delegatedState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return delegatedState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    public abstract ChangeApplier<K, N> getChangeApplier(ChangelogApplierFactory factory);
}
