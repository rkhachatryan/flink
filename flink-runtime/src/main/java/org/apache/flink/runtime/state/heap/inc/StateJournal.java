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

import org.apache.commons.collections.list.AbstractListDecorator;

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

	// if value comes from user we can't track it - so disable journaling // todo: better structure?
	boolean isStateExposed();

	/** Todo. */
	class ListJournal<T> implements StateJournal<List<T>, StateDiff.ListDiff<T>> {
		private final List<T> list;
		private final int initialLength;
		private final boolean isStateExposed;
		private boolean isCleared;

		ListJournal(List<T> list, boolean wasCleared, boolean isStateExposed) {
			this.list = list;
			this.isCleared = wasCleared;
			this.isStateExposed = isStateExposed;
			this.initialLength = list == null ? 0 : list.size(); // todo: verify; same for map?
		}

		@Override
		public List<T> getJournaledState() {
			// todo: override individual removes, updates from iterator?
			return list == null ? null : isStateExposed ? list : new AbstractListDecorator(list) {
				@Override
				public void clear() {
					super.clear();
					isCleared = true;
				}
			};
		}

		@Override
		public StateDiff.ListDiff<T> getDiff() {
			return new StateDiff.ListDiff<>(isStateExposed || isCleared ? list : list.subList(initialLength, list.size()), isStateExposed || isCleared);
		}

		@Override
		public boolean isStateExposed() {
			return isStateExposed;
		}
	}

	/** Todo. */
	class MapJournal<K, V> implements StateJournal<Map<K, V>, StateDiff.MapDiff<K, V>> {
		private final Map<K, V> map;
		private final JournalingMap<K, V> journalingMap;

		MapJournal(Map<K, V> map, boolean wasCleared, boolean isStateExposed) {
			this.map = map;
			this.journalingMap = isStateExposed ? null : new JournalingMap<>(map, wasCleared);
		}

		@Override
		public Map<K, V> getJournaledState() {
			return journalingMap == null ? map : journalingMap;
		}

		@Override
		public StateDiff.MapDiff<K, V> getDiff() {
			return journalingMap == null ? new StateDiff.MapDiff<>(true, map, Collections.emptySet()) : journalingMap.toDiff();
		}

		@Override
		public boolean isStateExposed() {
			return journalingMap == null;
		}

		@Override
		public String toString() {
			return String.format("map=%s, journaledMap=%s", map, journalingMap);
		}

	}

}
