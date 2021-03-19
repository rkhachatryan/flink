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

package org.apache.flink.state.changelog;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.Snapshotable;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

// T=SnapshotResult<KeyedStateHandle>
class PeriodicStateMaterializer<T extends StateObject> {
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicStateMaterializer.class);
    private static final CheckpointOptions CHECKPOINT_OPTIONS =
            CheckpointOptions.alignedNoTimeout(
                    CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());

    private final Snapshotable<T> backend;
    private final ScheduledExecutorService executor;
    private final int snapshotIntervalMs;
    private final Consumer<T> asyncPhaseResultAcceptor;
    private final CheckpointStreamFactory streamFactory;

    private State state = State.NOT_STARTED;
    private final AtomicLong runCounter = new AtomicLong(0);

    PeriodicStateMaterializer(
            Snapshotable<T> backend,
            Consumer<T> asyncPhaseResultAcceptor,
            CheckpointStorageWorkerView storageProvider,
            int snapshotIntervalMs) {
        this(
                backend,
                asyncPhaseResultAcceptor,
                storageProvider,
                snapshotIntervalMs,
                Executors.newSingleThreadScheduledExecutor());
    }

    PeriodicStateMaterializer(
            Snapshotable<T> backend,
            Consumer<T> asyncPhaseResultAcceptor,
            CheckpointStorageWorkerView storageProvider,
            int snapshotIntervalMs,
            ScheduledExecutorService executor) {
        this.executor = executor;
        this.backend = backend;
        this.snapshotIntervalMs = snapshotIntervalMs;
        this.asyncPhaseResultAcceptor = asyncPhaseResultAcceptor;
        this.streamFactory = ignoredScope -> storageProvider.createTaskOwnedStateStream();
    }

    public void start() {
        Preconditions.checkState(state == State.NOT_STARTED);
        state = State.STARTED;
        executor.scheduleAtFixedRate(
                this::takeSnapshot, snapshotIntervalMs, snapshotIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void takeSnapshot() {
        long runId = runCounter.getAndIncrement();
        long timestamp = System.currentTimeMillis();
        try {
            RunnableFuture<T> future =
                    backend.snapshot(runId, timestamp, streamFactory, CHECKPOINT_OPTIONS);
            future.run();
            asyncPhaseResultAcceptor.accept(future.get());
        } catch (Exception e) {
            LOG.error("unable to materialize state", e);
            // todo: handle error
            // todo: close stream
        }
    }

    boolean isStarted() {
        return state == State.STARTED;
    }

    public void shutdown() {
        if (state == State.CLOSED) {
            return;
        }
        state = State.CLOSED;
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                LOG.warn("Unable to terminate in 1s");
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while awaiting termination", e);
        }
    }

    private enum State {
        NOT_STARTED,
        STARTED,
        CLOSED
    }
}
