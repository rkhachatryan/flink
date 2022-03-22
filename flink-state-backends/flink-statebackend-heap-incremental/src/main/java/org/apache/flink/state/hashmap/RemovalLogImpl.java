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

package org.apache.flink.state.hashmap;

import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

@NotThreadSafe
class RemovalLogImpl<K, N> implements RemovalLog<K, N> {
    private final NavigableMap<Integer, Set<StateEntryRemoval<K, N>>> removedByVersion =
            new TreeMap<>();
    private Set<StateEntryRemoval<K, N>> current = new HashSet<>();

    @Override
    public void startNewVersion(int newVersion) {
        Preconditions.checkState(
                removedByVersion.isEmpty() || removedByVersion.lastKey() == newVersion - 2,
                "unexpected version: %s, %s",
                newVersion,
                removedByVersion);
        removedByVersion.put(newVersion - 1, current);
        current = new HashSet<>();
    }

    @Override
    public void confirmed(int version) {
        removedByVersion.headMap(version, true).clear();
    }

    @Override
    public void added(K k, N n) {
        current.remove(StateEntryRemoval.of(k, n));
    }

    @Override
    public void removed(K k, N n) {
        current.add(StateEntryRemoval.of(k, n));
    }

    @Override
    public NavigableMap<Integer, Set<StateEntryRemoval<K, N>>> snapshot() {
        // copy removedByVersion as we are going to modify it
        // however, we are not going to modify the maps it refers to
        return new TreeMap<>(removedByVersion);
    }
}
