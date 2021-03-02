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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

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

import static java.util.stream.Collectors.toList;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.CONFIRMED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.PENDING;
import static org.apache.flink.runtime.concurrent.FutureUtils.combineAll;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@NotThreadSafe
class FsStateChangelogWriter implements StateChangelogWriter<StateChangelogHandleStreamImpl> {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogWriter.class);

    private final UUID logId;
    private final KeyGroupRange keyGroupRange;
    private final StateChangeStore store;
    private final NavigableMap<SequenceNumber, StateChangeSet> changeSets = new TreeMap<>();
    private final long appendPersistThreshold;
    private List<StateChange> activeChangeSet = new ArrayList<>(); // todo: group by
    private SequenceNumber activeSequenceNumber = SequenceNumber.FIRST;
    private boolean closed;
    private long accumulatedBytes;
    private SequenceNumber nextAutoFlushFrom = activeSequenceNumber;

    FsStateChangelogWriter(
            UUID logId,
            KeyGroupRange keyGroupRange,
            StateChangeStore store,
            long appendPersistThreshold) {
        this.logId = logId;
        this.keyGroupRange = keyGroupRange;
        this.store = store;
        this.appendPersistThreshold = appendPersistThreshold;
    }

    @Override
    public void append(int keyGroup, byte[] value) throws IOException {
        LOG.trace("append to {}: keyGroup={} {} bytes", logId, keyGroup, value.length);
        checkState(!closed, "%s is closed", logId);
        activeChangeSet.add(new StateChange(keyGroup, value));
        accumulatedBytes += value.length;
        if (accumulatedBytes >= appendPersistThreshold) {
            LOG.debug(
                    "pre-emptively flush {}Mb of appended changes to the common store",
                    accumulatedBytes / 1024 / 1024);
            persistInternal(nextAutoFlushFrom, true);
            // considerations:
            // 0. can actually degrade performance by amplifying number of requests
            // 1. which range to persist?
            // 2. how to deal with retries/aborts?
        }
    }

    @Override
    public SequenceNumber lastAppendedSequenceNumber() {
        LOG.trace("query {} sqn: {}", logId, activeSequenceNumber);
        SequenceNumber tmp = activeSequenceNumber;
        rollover();
        return tmp;
    }

    @Override
    public CompletableFuture<StateChangelogHandleStreamImpl> persist(SequenceNumber from)
            throws IOException {
        LOG.debug(
                "persist {} starting from sqn {} (incl.), active sqn: {}",
                logId,
                from,
                activeSequenceNumber);
        checkNotNull(from);
        // with pre-flushes, backend will have an old sqn
        checkArgument(
                from.equals(SequenceNumber.FIRST)
                        || activeSequenceNumber.next().equals(from)
                        || changeSets.containsKey(from),
                "sequence number %s to persist from not in range (%s:%s/%s)",
                from,
                changeSets.isEmpty() ? null : changeSets.firstKey(),
                changeSets.isEmpty() ? null : changeSets.lastKey(),
                activeSequenceNumber.next());

        Collection<StateChangeSetUpload> uploads = persistInternal(from, false);
        return combineAll(
                        uploads.stream()
                                .map(StateChangeSetUpload::getStoreResultFuture)
                                .collect(toList()))
                .thenApply(this::buildHandle);
    }

    private Collection<StateChangeSetUpload> persistInternal(
            SequenceNumber from, boolean isPreUpload) throws IOException {
        rollover();
        nextAutoFlushFrom = activeSequenceNumber;
        Collected collected = collect(from, isPreUpload);
        if (!collected.toUpload.isEmpty()) {
            store.save(collected.toUpload);
        }
        accumulatedBytes = 0; // not always correct, but best effort
        return collected.toReturn;
    }

    private Collected collect(SequenceNumber from, boolean isPreUpload) {
        Collected result = new Collected();
        changeSets
                .tailMap(from, true)
                .values()
                .forEach(changeSet -> decideToCollect(changeSet, result, isPreUpload));
        result.toReturn.addAll(result.toUpload);
        LOG.debug("collected {}", result);
        return result;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void decideToCollect(StateChangeSet changeSet, Collected result, boolean isPreUpload) {
        if (changeSet.getStatus() == CONFIRMED) {
            result.toReturn.add(changeSet.getCurrentUpload());
        } else if (changeSet.getStatus() == PENDING) {
            changeSet.startUpload();
            if (!isPreUpload) {
                changeSet.associateUploadWithCheckpoint();
            }
            result.toUpload.add(changeSet.getCurrentUpload());
        } else if (isPreUpload) {
            // an upload was already started - and we don't want to re-upload now
        } else if (changeSet.isUploadAssociatedWithCheckpoint()) {
            // re-upload changes sent to JM as it can decide to discard them.
            // also re-upload any scheduled/uploading/uploaded changes even if they were not sent to
            // the JM yet - this can happen in the meantime from the future callback
            LOG.trace("re-upload {}", changeSet);
            changeSet.startUpload();
            result.toUpload.add(changeSet.getCurrentUpload());
        } else {
            // an upload was started pre-emptively - grab the result and force future callers to
            // re-upload
            changeSet.associateUploadWithCheckpoint();
            result.toReturn.add(changeSet.getCurrentUpload());
        }
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
        LOG.debug("confirm range {}..{} (inc./excl.) for {}", from, to, logId);
        changeSets
                .subMap(from, true, to, false)
                .forEach((sequenceNumber, stateChangeSet) -> stateChangeSet.setConfirmed());
    }

    @Override
    public void reset(SequenceNumber from, SequenceNumber to) {
        LOG.debug("reset range {}..{} (inc./excl.) for {}", from, to, logId);
        changeSets
                .subMap(from, true, to, false)
                .forEach((key, changeSet) -> changeSet.getCurrentUpload().cancel());
        // todo: review the comment below
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
        LOG.debug("truncate {} to sqn {} (excl.)", logId, to);
        if (to.compareTo(activeSequenceNumber) > 0) {
            // can happen if client calls truncate(prevSqn.next())
            rollover();
        }
        NavigableMap<SequenceNumber, StateChangeSet> headMap = changeSets.headMap(to, false);
        headMap.values().forEach(StateChangeSet::setTruncated);
        headMap.clear();
    }

    private void rollover() {
        if (activeChangeSet.isEmpty()) {
            return;
        }
        activeSequenceNumber = activeSequenceNumber.next();
        LOG.debug("bump active sqn to {}", activeSequenceNumber);
        changeSets.put(
                activeSequenceNumber,
                new StateChangeSet(logId, activeSequenceNumber, activeChangeSet, PENDING));
        activeChangeSet = new ArrayList<>();
    }

    private StateChangelogHandleStreamImpl buildHandle(Collection<StoreResult> results) {
        List<Tuple3<StreamStateHandle, Long, Long>> sorted =
                results.stream()
                        // can't assume order across different handles because of retries and aborts
                        .sorted(Comparator.comparing(StoreResult::getSequenceNumber))
                        .map(
                                up ->
                                        Tuple3.of(
                                                up.getStreamStateHandle(),
                                                up.getOffset(),
                                                up.getSize()))
                        .collect(toList());
        // todo in MVP: replace old handles with placeholders (but maybe in the backend)
        return new StateChangelogHandleStreamImpl(sorted, keyGroupRange);
    }

    @VisibleForTesting
    SequenceNumber lastAppendedSqnUnsafe() {
        return activeSequenceNumber;
    }

    private static class Collected {
        private final Collection<StateChangeSetUpload> toUpload = new ArrayList<>();
        private final Collection<StateChangeSetUpload> toReturn = new ArrayList<>();

        @Override
        public String toString() {
            return String.format(
                    "changes to upload: %d (%dMb), to return total: %d (%dMb)",
                    toUpload.size(),
                    toUpload.stream().mapToLong(StateChangeSetUpload::getSize).sum() / 1024 / 1024,
                    toReturn.size(),
                    toReturn.stream().mapToLong(StateChangeSetUpload::getSize).sum() / 1024 / 1024);
        }
    }
}
