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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@SuppressWarnings("NullableProblems")
class JournalingMap<K, V> implements Map<K, V> {
	private final Map<K, V> map;
	private final Set<K> changed = new HashSet<>();
	private final Set<K> removed = new HashSet<>();
	private boolean cleared;
	private boolean exposed;

	JournalingMap(Map<K, V> map, boolean cleared) {
		this.map = map;
		this.cleared = cleared;
	}

	// todo: optimize (don't record if nothing changed)
	// todo: allow modifications
	// todo: include journal into equals?
	public int size() {
		return map.size();
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	public V get(Object key) {
		return map.get(key);
	}

	public V put(K key, V value) {
		recordAdd(key);
		return map.put(key, value);
	}

	public V remove(Object key) {
		//noinspection unchecked
		recordRemove((K) key);
		return map.remove(key);
	}

	private void recordAdd(K key) {
		removed.remove(key);
		changed.add(key);
	}

	private void recordRemove(K key) {
		removed.add(key);
		changed.remove(key);
	}

	public void putAll(Map<? extends K, ? extends V> all) {
		Set<? extends K> keys = all.keySet();
		changed.addAll(keys);
		removed.removeAll(keys);
		map.putAll(all);
	}

	public void clear() {
		recordClear();
		map.clear();
	}

	public Set<K> keySet() {
		recordExposed();
		return map.keySet();
	}

	public Collection<V> values() {
		recordExposed();
		return map.values();
	}

	public Set<Entry<K, V>> entrySet() {
		recordExposed();
		return map.entrySet();
	}

	private void recordExposed() {
		// todo: efficiency: track changes to the exposed part instead of giving up diff
		// todo: deduplicate this from upper level (journal)
		cleared = true;
		exposed = true;
		changed.clear();
		removed.clear();
	}

	@Override
	@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
	public boolean equals(Object o) {
		return map.equals(o);
	}

	@Override
	public int hashCode() {
		return map.hashCode();
	}

	@Override
	public String toString() {
		return String.format("wrapping map: %s", map);
	}

	public V getOrDefault(Object key, V defaultValue) {
		return map.getOrDefault(key, defaultValue);
	}

	public void forEach(BiConsumer<? super K, ? super V> action) {
		map.forEach(action);
	}

	public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
		recordExposed();
		map.replaceAll(function);
	}

	public V putIfAbsent(K key, V value) {
		if (map.containsKey(key)) {
			recordAdd(key);
		}
		return map.putIfAbsent(key, value);
	}

	public boolean remove(Object key, Object value) {
		boolean remove = map.remove(key, value);
		if (remove) {
			//noinspection unchecked
			recordRemove((K) key);
		}
		return remove;
	}

	public boolean replace(K key, V oldValue, V newValue) {
		boolean replace = map.replace(key, oldValue, newValue);
		if (replace) {
			recordAdd(key);
		}
		return replace;
	}

	public V replace(K key, V value) {
		if (map.containsKey(key)) {
			recordAdd(key);
		}
		return map.replace(key, value);
	}

	public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
		return map.computeIfAbsent(key, k -> {
			V v = mappingFunction.apply(k);
			if (v != null) {
				recordAdd(key);
			}
			return v;
		});
	}

	public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		return map.computeIfPresent(key, (k, v) -> {
			V v1 = remappingFunction.apply(k, v);
			if (v1 == null) {
				recordRemove(k);
			} else {
				recordAdd(k);
			}
			return v1;
		});
	}

	public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		V compute = map.compute(key, remappingFunction);
		if (compute == null) {
			recordRemove(key);
		} else {
			recordAdd(key);
		}
		return compute;
	}

	public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
		recordExposed();
		return map.merge(key, value, remappingFunction);
	}

	void recordClear() {
		cleared = true;
		changed.clear();
		removed.clear();
	}

	StateDiff.MapDiff<K, V> toDiff() {
		if (exposed) {
			// todo: is copying needed? should it be done in sync phase? also for list
			return new StateDiff.MapDiff<>(true, new HashMap<>(map), Collections.emptySet());
		} else {
			HashMap<K, V> up = new HashMap<>();
			for (K k : this.changed) {
				up.put(k, map.get(k));
			}
			return new StateDiff.MapDiff<>(cleared, up, removed);
		}
	}

	Map<K, V> unwrap() {
		return map;
	}

	public boolean isStateExposed() {
		return exposed;
	}

	public boolean isCleared() {
		return cleared;
	}
}
