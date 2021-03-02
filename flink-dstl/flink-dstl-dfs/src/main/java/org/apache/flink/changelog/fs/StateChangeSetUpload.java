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

import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkState;

@ThreadSafe
// todo: javadoc
class StateChangeSetUpload {
    private final long size;
    private final UUID logId;
    private final SequenceNumber sequenceNumber;
    private final CompletableFuture<StoreResult> storeResultFuture;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.PENDING);
    private List<StateChange> changes;

    private enum Status {
        PENDING,
        UPLOADING,
        COMPLETED // uploaded/cancelled/failed
    }

    StateChangeSetUpload(
            List<StateChange> changes, long size, UUID logId, SequenceNumber sequenceNumber) {
        this.changes = changes;
        this.sequenceNumber = sequenceNumber;
        this.storeResultFuture = new CompletableFuture<>();
        this.size = size;
        this.logId = logId;
    }

    public void cancel() {
        fail(new CancellationException());
    }

    public void fail(Throwable error) {
        if (status.compareAndSet(Status.PENDING, Status.COMPLETED)) {
            storeResultFuture.completeExceptionally(error);
            clear();
        }
    }

    public boolean startUpload() {
        // prevent clearing during the upload
        return status.compareAndSet(Status.PENDING, Status.UPLOADING);
    }

    public void complete(StoreResult storeResult) {
        if (status.compareAndSet(Status.UPLOADING, Status.COMPLETED)) {
            storeResultFuture.complete(storeResult);
        } else {
            // todo: cleanup uploaded state
        }
        clear();
    }

    private void clear() {
        checkState(status.get() != Status.UPLOADING);
        changes = null;
    }

    public long getSize() {
        return size;
    }

    public List<StateChange> getChanges() {
        return changes;
    }

    public UUID getLogId() {
        return logId;
    }

    public SequenceNumber getSequenceNumber() {
        return sequenceNumber;
    }

    public CompletableFuture<StoreResult> getStoreResultFuture() {
        return storeResultFuture;
    }
}
