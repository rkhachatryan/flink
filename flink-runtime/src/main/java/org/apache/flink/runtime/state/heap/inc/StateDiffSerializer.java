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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.heap.inc.StateDiff.ListDiff;
import org.apache.flink.runtime.state.heap.inc.StateDiff.MapDiff;
import org.apache.flink.runtime.state.heap.inc.StateDiff.ReplacingDiff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface StateDiffSerializer<S, D extends StateDiff<S>> {

	static <S> StateDiffSerializer<S, ReplacingDiff<S>> replacing(TypeSerializer<S> typeSerializer) {
		return new StateDiffSerializer<S, ReplacingDiff<S>>(){
			@Override
			public void serialize(ReplacingDiff<S> diff, DataOutputView dov) throws IOException {
				typeSerializer.serialize(diff.getState(), dov);
			}

			@Override
			public ReplacingDiff<S> deserialize(DataInputView div) throws IOException {
				return new ReplacingDiff<>(typeSerializer.deserialize(div));
			}
		};
	}

	void serialize(D diff, DataOutputView dov) throws IOException;

	D deserialize(DataInputView div) throws IOException;

	class MapDiffSerializer<K, V> implements StateDiffSerializer<Map<K, V>, MapDiff<K, V>> {
		private static final Logger LOG = LoggerFactory.getLogger(MapDiffSerializer.class);
		private final MapSerializer<K, V> mapSerializer;

		public MapDiffSerializer(MapSerializer<K, V> mapSerializer) {
			this.mapSerializer = mapSerializer;
		}

		@Override
		public void serialize(MapDiff<K, V> diff, DataOutputView dov) throws IOException {
			LOG.trace("serialize: {}", diff);
			dov.writeBoolean(diff.wasCleared);
			dov.writeInt(diff.removed.size());
			for (K k : diff.removed) {
				mapSerializer.getKeySerializer().serialize(k, dov);
			}
			mapSerializer.serialize(diff.updated, dov);
		}

		@Override
		public MapDiff<K, V> deserialize(DataInputView div) throws IOException {
			boolean wasCleared = div.readBoolean();
			Set<K> removed = new HashSet<>();
			int numRemoved = div.readInt();
			for (int i = 0; i < numRemoved; i++) {
				removed.add(mapSerializer.getKeySerializer().deserialize(div));
			}
			Map<K, V> updated = mapSerializer.deserialize(div);
			return new MapDiff<>(wasCleared, updated, removed);
		}
	}

	class ListDiffSerializer<T> implements StateDiffSerializer<List<T>, ListDiff<T>> {
		private static final Logger LOG = LoggerFactory.getLogger(ListDiffSerializer.class);

		private final ListSerializer<T> listSerializer;

		public ListDiffSerializer(ListSerializer<T> listSerializer) {
			this.listSerializer = listSerializer;
		}

		@Override
		public void serialize(ListDiff<T> diff, DataOutputView dov) throws IOException {
			LOG.trace("serialize: {}", diff);
			listSerializer.serialize(diff.delta, dov);
			dov.writeBoolean(diff.wasCleared);
		}

		@Override
		public ListDiff<T> deserialize(DataInputView div) throws IOException {
			return new ListDiff<>(listSerializer.deserialize(div), div.readBoolean());
		}
	}
}
