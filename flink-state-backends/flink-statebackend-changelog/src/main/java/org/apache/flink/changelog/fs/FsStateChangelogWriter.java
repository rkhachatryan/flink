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

package org.apache.flink.changelog.fs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.SequenceNumber;
import org.apache.flink.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateChange;
import org.apache.flink.runtime.state.StateChangelogHandleStreamImpl;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.changelog.fs.StateChangeSet.Status.PENDING;
import static org.apache.flink.runtime.concurrent.FutureUtils.combineAll;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@NotThreadSafe
class FsStateChangelogWriter implements StateChangelogWriter<StateChangelogHandleStreamImpl> {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogWriter.class);

    private final UUID logId;
    private final KeyGroupRange keyGroupRange;
    private final StateChangeStore store;
    private final NavigableMap<SequenceNumber, StateChangeSet> changeSets = new TreeMap<>();
    private List<StateChange> activeChangeSet = new ArrayList<>();
    private SequenceNumber currentSqn = SequenceNumber.of(0L);
    private boolean closed;

    FsStateChangelogWriter(UUID logId, KeyGroupRange keyGroupRange, StateChangeStore store) {
        this.logId = logId;
        this.keyGroupRange = keyGroupRange;
        this.store = store;
    }

    @Override
    public void append(int keyGroup, byte[] value) {
        LOG.trace("append to {}: keyGroup={} {} bytes", logId, keyGroup, value.length);
        checkState(!closed, "%s is closed", logId);
        activeChangeSet.add(new StateChange(keyGroup, value));
        // size threshold could be added to call persist when reached. considerations:
        // 0. can actually degrade performance by amplifying number of requests
        // 1. which range to persist?
        // 2. how to deal with retries/aborts?
    }

    @Override
    public SequenceNumber lastAppendedSqn() {
        LOG.trace("query {} sqn: {}", logId, currentSqn);
        SequenceNumber tmp = currentSqn;
        rollover();
        return tmp;
    }

    @Override
    public CompletableFuture<StateChangelogHandleStreamImpl> persist(SequenceNumber from)
            throws IOException {
        LOG.debug("persist {} from {}", logId, from);
        checkNotNull(from);
        checkState(
                changeSets.containsKey(from),
                "sequence number %s to persist from not in range (%s:%s)",
                from,
                changeSets.firstKey(),
                changeSets.lastKey());
        rollover();

        Collection<StateChangeSet> upload = new ArrayList<>();
        Collection<StateChangeSet> retry = new ArrayList<>();
        Collection<StateChangeSet> snapshot = new ArrayList<>();
        changeSets
                .tailMap(from, true)
                .values()
                .forEach(
                        changeSet -> {
                            if (changeSet.isConfirmed()) {
                                snapshot.add(changeSet);
                            } else if (changeSet.setScheduled()) {
                                upload.add(changeSet);
                            } else {
                                // we also re-upload any scheduled/uploading/uploaded changes
                                // even if they were not sent to the JM yet because this can happen
                                // in the meantime and then JM can decide to discard them
                                retry.add(changeSet.forRetry());
                            }
                        });
        upload.addAll(retry);
        snapshot.addAll(upload);
        retry.forEach(changeSet -> changeSets.put(changeSet.getSequenceNumber(), changeSet));
        store.save(upload);
        return asHandle(snapshot);
    }

    @Override
    public void close() {
        LOG.debug("close {}", logId);
        checkState(!closed);
        closed = true;
        activeChangeSet.clear();
        changeSets.values().forEach(StateChangeSet::setCancelled);
        // todo in MVP or later: cleanup if transition succeeded and had non-shared state
        changeSets.clear();
        // the store is closed from the owning FsStateChangelogClient
    }

    @Override
    public void confirm(SequenceNumber from, SequenceNumber to) {
        LOG.debug("confirm {} from {} to {}", logId, from, to);
        changeSets.subMap(from, true, to, false).values().forEach(StateChangeSet::setConfirmed);
    }

    @Override
    public void reset(SequenceNumber from, SequenceNumber to) {
        LOG.debug("reset {} from {} to {}", logId, from, to);
        changeSets.subMap(from, to).forEach((key, value) -> value.setAborted());
        // todo in MVP or later: cleanup if change to aborted succeeded and had non-shared state
        // For now, relying on manual cleanup.
        // If a checkpoint that is aborted uses the changes uploaded for another checkpoint
        // which was completed on JM but not confirmed to this TM
        // then discarding those changes would invalidate that previously completed checkpoint.
        // Solution:
        // 1. pass last completed checkpoint id in barriers, trigger RPC, and abort RPC
        // 2. confirm for the id above
        // 3. make sure that at most 1 checkpoint in flight (CC config)
    }

    @Override
    public void truncate(SequenceNumber to) {
        LOG.debug("truncate {} to {}", logId, to);
        rollover();
        NavigableMap<SequenceNumber, StateChangeSet> headMap = changeSets.headMap(to, false);
        headMap.values().forEach(StateChangeSet::setTruncated);
        headMap.clear();
    }

    private void rollover() {
        changeSets.put(currentSqn, new StateChangeSet(logId, currentSqn, activeChangeSet, PENDING));
        activeChangeSet = new ArrayList<>();
        currentSqn = currentSqn.next();
    }

    private CompletableFuture<StateChangelogHandleStreamImpl> asHandle(
            Collection<StateChangeSet> snapshot) {
        return combineAll(
                        snapshot.stream()
                                .map(StateChangeSet::getStoreResult)
                                .collect(Collectors.toList()))
                .thenApply(uploaded -> buildHandle(snapshot, uploaded));
    }

    private StateChangelogHandleStreamImpl buildHandle(
            Collection<StateChangeSet> changes, Collection<StoreResult> results) {
        List<Tuple2<StreamStateHandle, Long>> sorted =
                results.stream()
                        // can't assume order across different handles because of retries and aborts
                        .sorted(Comparator.comparing(StoreResult::getSequenceNumber))
                        .map(up -> Tuple2.of(up.getStreamStateHandle(), up.getOffset()))
                        .collect(Collectors.toList());
        changes.forEach(StateChangeSet::setSentToJm);
        // todo in MVP: replace old handles with placeholders (but maybe in the backend)
        return new StateChangelogHandleStreamImpl(sorted, keyGroupRange);
    }
}
