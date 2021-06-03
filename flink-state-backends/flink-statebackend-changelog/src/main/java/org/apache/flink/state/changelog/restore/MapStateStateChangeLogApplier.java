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

import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;

class MapStateStateChangeLogApplier<K, N, UK, UV> extends KvStateStateChangeApplier<K, N> {
    private final InternalMapState<K, N, UK, UV> mapState;

    protected MapStateStateChangeLogApplier(
            InternalMapState<K, N, UK, UV> mapState, InternalKeyContext<K> keyContext) {
        super(keyContext);
        this.mapState = mapState;
    }

    @Override
    protected void applyStateUpdate(DataInputView in) throws Exception {
        mapState.putAll(getMapSerializer().deserialize(in));
    }

    @Override
    protected void applyStateUpdateInternal(DataInputView in) throws Exception {
        applyStateUpdate(in);
    }

    @Override
    protected void applyStateAdded(DataInputView in) throws Exception {
        mapState.putAll(getMapSerializer().deserialize(in));
    }

    @Override
    protected void applyStateElementAdded(DataInputView in) throws Exception {
        UK k = getMapSerializer().getKeySerializer().deserialize(in);
        UV v = getMapSerializer().getValueSerializer().deserialize(in);
        mapState.put(k, v);
    }

    @Override
    protected void applyStateElementAddedOrUpdated(DataInputView in) throws Exception {
        applyStateElementAdded(in);
    }

    @Override
    protected InternalKvState<K, N, ?> getState() {
        return mapState;
    }

    @Override
    protected void applyNameSpaceMerge(DataInputView in) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void applyStateElementRemoved(DataInputView in) throws Exception {
        mapState.remove(getMapSerializer().getKeySerializer().deserialize(in));
    }

    private MapSerializer<UK, UV> getMapSerializer() {
        return (MapSerializer<UK, UV>) mapState.getValueSerializer();
    }
}
