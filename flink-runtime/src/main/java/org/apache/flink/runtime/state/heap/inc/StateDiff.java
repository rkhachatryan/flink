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

import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * StateDiff.
 *
 * @param <S> state type
 */
public interface StateDiff<S> {
	S apply(S state);

	class ReplacingDiff<T> implements StateDiff<T> {
		private final T state;

		public ReplacingDiff(T state) {
			this.state = state;
		}

		@Override
		public T apply(T ignore) {
			return state;
		}

		public T getState() {
			return state;
		}

		@Override
		public String toString() {
			return "replacing diff, state=" + state;
		}
	}

	class ListDiff<T> implements StateDiff<List<T>> {
		public final List<T> delta;
		public final boolean wasCleared;

		public ListDiff(List<T> delta, boolean wasCleared) {
			this.delta = delta;
			this.wasCleared = wasCleared;
		}

		@Override
		public List<T> apply(List<T> state) {
			if (state == null) {
				return delta;
			} else {
				if (wasCleared) {
					state.clear();
				}
				state.addAll(delta);
				return state;
			}
		}

		@Override
		public String toString() {
			return String.format("cleared: %s, delta: %s .. %s", wasCleared, delta.get(0), delta.get(delta.size() - 1));
		}
	}

	class MapDiff<K, V> implements StateDiff<Map<K, V>> {
		final boolean wasCleared;
		public final Map<K, V> updated;
		public final Set<K> removed;

		public MapDiff(boolean wasCleared, Map<K, V> updated, Set<K> removed) {
			this.wasCleared = wasCleared;
			this.updated = updated;
			this.removed = removed;
			if (wasCleared) {
				// todo: uncomment; these checks lead to hiding of errors in ha-e2e tests thanks to recovery
				// Preconditions.checkArgument(full.equals(updated), "wasCleared but full ne updated: %s || %s", full, updated);
				// Preconditions.checkArgument(removed.isEmpty(), "wasCleared but removed not empty");
			}
		}

		@Override
		public Map<K, V> apply(Map<K, V> state) {
			// todo: wrap?
			// todo: confirm null is possible
			if (state == null) {
				Preconditions.checkState(removed == null || removed.isEmpty(), "passed state is empty, but there are keys to remove: %s", removed);
				return updated;
			} else {
				if (wasCleared) {
					state.clear();
				} else {
					removed.forEach(state::remove);
				}
				updated.forEach(state::put);
				return state;
			}
		}

		@Override
		public String toString() {
			return String.format("wasCleared=%s, updated=%s, removed=%s", wasCleared, updated, removed);
		}
	}
}
