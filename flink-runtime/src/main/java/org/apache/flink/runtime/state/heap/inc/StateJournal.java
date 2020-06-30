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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * StateJournal.
 *
 * @param <S>
 * @param <D>
 */
public interface StateJournal<S, D extends StateDiff<S>> {

	S getJournaledState();

	D getDiff();

	// if value comes from user we can't track it (and so can disable journaling)
	boolean isStateExposed();

	boolean wasCleared();

	/**
	 * {@link StateJournal} for {@link List}.
	 */
	class ListJournal<T> implements StateJournal<List<T>, StateDiff.ListDiff<T>> {
		private final List<T> originalList;
		private final JournalingList<T> journalingList;
		private final boolean isStateExposed;

		ListJournal(List<T> list, boolean wasCleared, boolean isStateExposed) {
			list = list instanceof JournalingList ? ((JournalingList<T>) list).unwrap() : list;
			this.isStateExposed = isStateExposed;
			this.originalList = list;
			this.journalingList = list == null || isStateExposed ? null : new JournalingList<>(list, wasCleared);
		}

		@Override
		@SuppressWarnings("unchecked")
		public List<T> getJournaledState() {
			return journalingList == null ? originalList : journalingList;
		}

		@Override
		public StateDiff.ListDiff<T> getDiff() {
			return journalingList == null ? new StateDiff.ListDiff<>(originalList, true) : journalingList.toDiff();
		}

		@Override
		public boolean isStateExposed() {
			return isStateExposed || (journalingList != null && journalingList.isStateExposed());
		}

		@Override
		public boolean wasCleared() {
			return journalingList == null || journalingList.isCleared();
		}
	}

	/**
	 * {@link StateJournal} for {@link Map}.
	 */
	class MapJournal<K, V> implements StateJournal<Map<K, V>, StateDiff.MapDiff<K, V>> {
		private final Map<K, V> originalMap;
		private final JournalingMap<K, V> journalingMap;
		private final boolean isStateExposed;

		MapJournal(Map<K, V> map, boolean wasCleared, boolean isStateExposed) {
			map = map instanceof JournalingMap ? ((JournalingMap<K, V>) map).unwrap() : map;
			this.isStateExposed = isStateExposed;
			this.originalMap = map;
			this.journalingMap = isStateExposed ? null : new JournalingMap<>(map, wasCleared);
		}

		@Override
		public Map<K, V> getJournaledState() {
			return journalingMap == null ? originalMap : journalingMap;
		}

		@Override
		public StateDiff.MapDiff<K, V> getDiff() {
			return journalingMap == null ? new StateDiff.MapDiff<>(true, originalMap, Collections.emptySet()) : journalingMap.toDiff();
		}

		@Override
		public boolean isStateExposed() {
			return isStateExposed || (journalingMap != null && journalingMap.isStateExposed());
		}

		@Override
		public boolean wasCleared() {
			return journalingMap == null || journalingMap.isCleared();
		}

		@Override
		public String toString() {
			return String.format("map=%s, journaledMap=%s", originalMap, journalingMap);
		}
	}
}
