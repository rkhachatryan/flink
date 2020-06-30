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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.heap.inc.StateDiffSerializer.ListDiffSerializer;
import org.apache.flink.runtime.state.heap.inc.StateDiffSerializer.MapDiffSerializer;
import org.apache.flink.runtime.state.heap.inc.StateJournal.ListJournal;
import org.apache.flink.runtime.state.heap.inc.StateJournal.MapJournal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Metadata about incremental state.
 *
 * @param <S> type of state
 * @param <D> type of state diff
 * @param <J> type of state journal
 */
public class IncrementalStateMetaInfo<S, D extends StateDiff<S>, J extends StateJournal<S, D>> {
	private static final Logger LOG = LoggerFactory.getLogger(IncrementalStateMetaInfo.class);

	private final StateDiffSerializer<S, D> diffSerializer;
	private final StateJournalFactory<S, D, J> journalFactory;

	private IncrementalStateMetaInfo(StateDiffSerializer<S, D> diffSerializer, StateJournalFactory<S, D, J> journalFactory) {
		this.diffSerializer = diffSerializer;
		this.journalFactory = journalFactory;
	}

	public StateDiffSerializer<S, D> getDiffSerializer() {
		return diffSerializer;
	}

	public StateJournalFactory<S, D, J> getJournalFactory() {
		return journalFactory;
	}

	public static <T> IncrementalStateMetaInfo<List<T>, StateDiff.ListDiff<T>, ListJournal<T>> forList(ListSerializer<T> serializer) {
		//noinspection Convert2Diamond
		return new IncrementalStateMetaInfo<List<T>, StateDiff.ListDiff<T>, ListJournal<T>>(new ListDiffSerializer<>(serializer), ListJournal::new);
	}

	public static <K, V> IncrementalStateMetaInfo<Map<K, V>, StateDiff.MapDiff<K, V>, MapJournal<K, V>> forMap(MapSerializer<K, V> serializer) {
		//noinspection Convert2Diamond
		return new IncrementalStateMetaInfo<Map<K, V>, StateDiff.MapDiff<K, V>, MapJournal<K, V>>(new MapDiffSerializer<>(serializer), MapJournal::new);
	}

	public static <S> IncrementalStateMetaInfo<S, ?, ?> fromStateDescriptor(StateDescriptor<?, S> desc) {
		return fromStateDescriptor(desc.getType(), desc.getSerializer(), desc.getName());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <S> IncrementalStateMetaInfo<S, ?, ?> fromStateDescriptor(StateDescriptor.Type type, TypeSerializer<S> serializer, String name) {
		LOG.trace("Requested IncMeta for {} {} {}", type, name, serializer);
		switch (type) {
			case MAP:
				return serializer instanceof MapSerializer ? forMap((MapSerializer) serializer) : replacing(serializer); // fixme: other serializers
			case LIST:
				return serializer instanceof ListSerializer ? forList((ListSerializer) serializer) : replacing(serializer); // fixme: other serializers (ArrayListSerializer)
			// todo: folding and co?
			// todo: value?
			default:
				return replacing(serializer);
		}
	}

	public static <S> IncrementalStateMetaInfo<S, ?, ?> replacing(TypeSerializer<S> serializer) {
		return new IncrementalStateMetaInfo<>(StateDiffSerializer.replacing(serializer), StateJournalFactory.replacing());
	}
}
