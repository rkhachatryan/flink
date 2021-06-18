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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.state.changelog.StateChangeOperation;

abstract class KvStateStateChangeApplier<K, N> implements StateChangeApplier {
    private final InternalKeyContext<K> keyContext;

    protected abstract InternalKvState<K, N, ?> getState();

    protected KvStateStateChangeApplier(InternalKeyContext<K> keyContext) {
        this.keyContext = keyContext;
    }

    @Override
    public void apply(StateChangeOperation operation, DataInputView in) throws Exception {
        K key = getState().getKeySerializer().deserialize(in);
        keyContext.setCurrentKey(key);
        keyContext.setCurrentKeyGroupIndex(
                KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups()));
        getState().setCurrentNamespace(getState().getNamespaceSerializer().deserialize(in));
        switch (operation) {
            case ADD:
                applyStateAdded(in);
                return;
            case CLEAR:
                applyStateClear();
                return;
            case ADD_ELEMENT:
                applyStateElementAdded(in);
                return;
            case ADD_OR_UPDATE_ELEMENT:
                applyStateElementAddedOrUpdated(in);
                return;
            case SET:
                applyStateUpdate(in);
                return;
            case SET_INTERNAL:
                applyStateUpdateInternal(in);
                return;
            case MERGE_NS:
                applyNamespaceMerged(in);
                return;
            case REMOVE_ELEMENT:
                applyStateElementRemoved(in);
                return;
            default:
                throw new IllegalArgumentException("Unknown state change operation: " + operation);
        }
    }

    protected abstract void applyNamespaceMerged(DataInputView in) throws Exception;

    protected abstract void applyStateAdded(DataInputView in) throws Exception;

    protected void applyStateClear() {
        getState().clear();
    }

    protected abstract void applyStateElementAdded(DataInputView in) throws Exception;

    protected abstract void applyStateElementAddedOrUpdated(DataInputView in) throws Exception;

    protected abstract void applyStateElementRemoved(DataInputView in) throws Exception;

    protected abstract void applyStateUpdate(DataInputView in) throws Exception;

    protected abstract void applyStateUpdateInternal(DataInputView in) throws Exception;
}
