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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import static org.apache.flink.runtime.state.heap.inc.IncrementalStateMetaInfo.fromStateDescriptor;

class IncrementalRegisteredKeyValueStateBackendMetaInfo<N, S> extends RegisteredKeyValueStateBackendMetaInfo<N, S> {

	IncrementalRegisteredKeyValueStateBackendMetaInfo(StateMetaInfoSnapshot metaInfoSnapshot) {
		super(metaInfoSnapshot);
		this.incrementalStateMetaInfo = stateSerializerProvider.getPreviousSerializerSnapshot() == null ?
			IncrementalStateMetaInfo.replacing(stateSerializerProvider.currentSchemaSerializer()) : // todo: always use this + fromStateDesc?
			fromStateDescriptor(super.stateType, stateSerializerProvider.getPreviousSerializerSnapshot().restoreSerializer(), name);
	}

	IncrementalRegisteredKeyValueStateBackendMetaInfo(
			StateDescriptor<?, S> stateDesc,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer,
			StateSnapshotTransformer.StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {
		super(
			stateDesc.getType(),
			stateDesc.getName(),
			namespaceSerializer,
			stateSerializer,
			stateSnapshotTransformFactory);
		this.incrementalStateMetaInfo = stateSerializerProvider.getPreviousSerializerSnapshot() == null ?
			fromStateDescriptor(stateDesc) :
			fromStateDescriptor(stateType, stateSerializerProvider.getPreviousSerializerSnapshot().restoreSerializer(), name);
	}

	private IncrementalStateMetaInfo<S, ?, ?> incrementalStateMetaInfo; // todo: eq/hc/snapshot/...

	public IncrementalStateMetaInfo<S, ?, ?> getIncrementalStateMetaInfo() {
		return incrementalStateMetaInfo;
	}

	public void setIncrementalStateMetaInfo(IncrementalStateMetaInfo<S, ?, ?> incrementalStateMetaInfo) {
		this.incrementalStateMetaInfo = incrementalStateMetaInfo;
	}
}
