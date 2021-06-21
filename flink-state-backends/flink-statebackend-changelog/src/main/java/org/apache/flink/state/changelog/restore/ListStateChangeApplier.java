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

package org.apache.flink.state.changelog.restore;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class ListStateChangeApplier<K, N, T> extends KvStateStateChangeApplier<K, N> {
    private final InternalListState<K, N, T> state;

    protected ListStateChangeApplier(
            InternalKeyContext<K> keyContext, InternalListState<K, N, T> state) {
        super(keyContext);
        this.state = state;
    }

    @Override
    protected void applyStateUpdate(DataInputView in) throws Exception {
        state.update(state.getValueSerializer().deserialize(in));
    }

    @Override
    protected void applyStateUpdateInternal(DataInputView in) throws Exception {
        state.updateInternal(state.getValueSerializer().deserialize(in));
    }

    @Override
    protected void applyStateAdded(DataInputView in) throws Exception {
        state.addAll(state.getValueSerializer().deserialize(in));
    }

    @Override
    protected void applyStateElementAdded(DataInputView in) throws Exception {
        TypeSerializer<List<T>> valueSerializer = state.getValueSerializer();
        if (valueSerializer instanceof ListSerializer) {
            state.add(((ListSerializer<T>) valueSerializer).getElementSerializer().deserialize(in));
        } else {
            state.addAll(valueSerializer.deserialize(in));
        }
    }

    @Override
    protected void applyStateElementAddedOrUpdated(DataInputView in) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected InternalKvState<K, N, ?> getState() {
        return state;
    }

    @Override
    protected void applyNamespaceMerged(DataInputView in) throws Exception {
        N target = state.getNamespaceSerializer().deserialize(in);
        int sourcesSize = in.readInt();
        Collection<N> sources = new ArrayList<>(sourcesSize);
        for (int i = 0; i < sourcesSize; i++) {
            sources.add(state.getNamespaceSerializer().deserialize(in));
        }
        state.mergeNamespaces(target, sources);
    }

    @Override
    protected void applyStateElementRemoved(DataInputView in) {
        throw new UnsupportedOperationException();
    }
}
