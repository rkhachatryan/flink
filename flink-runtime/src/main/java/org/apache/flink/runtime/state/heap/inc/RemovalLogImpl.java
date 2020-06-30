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

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

@NotThreadSafe
@SuppressWarnings("Java8MapApi")
class RemovalLogImpl<K, N> implements RemovalLog<K, N> {
	private TreeMap<Integer, Map<K, Set<N>>> removedByVersion = new TreeMap<>();
	private Map<K, Set<N>> current = new HashMap<>();
	private final AtomicInteger lastConfirmedVersion = new AtomicInteger(-1);

	@Override
	public void startNewVersion(int newVersion) {
		Preconditions.checkState(removedByVersion.isEmpty() || removedByVersion.lastKey() == newVersion - 2,
			"unexpected version: %s, %s", newVersion, removedByVersion);
		removedByVersion.put(newVersion - 1, current);
		current = new HashMap<>();
	}

	@Override
	public void added(K k, N n) {
		Set<N> ns = current.get(k);
		if (ns != null) {
			ns.remove(n);
		}
	}

	@Override
	public void removed(K k, N n) {
		Set<N> ns = current.get(k);
		if (ns == null) {
			ns = new HashSet<>();
			current.put(k, ns);
		}
		ns.add(n);
	}

	@Override
	public void confirmed(int version) {
		this.lastConfirmedVersion.getAndUpdate(v -> Math.max(v, version));
	}

	@Override
	public void truncate() {
		int fromKey = lastConfirmedVersion.get();
		removedByVersion = new TreeMap<>(removedByVersion.tailMap(fromKey, false));
	}

	@Override
	public Collection<Map<K, Set<N>>> collect(int upToVersion) {
		// copy removedByVersion as we are going to modify it
		// however, we are not going to modify the maps it refers to
		return new ArrayList<>(removedByVersion.headMap(upToVersion, true).values());
	}
}
