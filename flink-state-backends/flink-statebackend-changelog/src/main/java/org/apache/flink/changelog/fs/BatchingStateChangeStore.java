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

import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link StateChangeStore} that waits for some configured amount of time before passing the
 * accumulated state changes to the actual store.
 */
class BatchingStateChangeStore implements StateChangeStore {
    private static final Logger LOG = LoggerFactory.getLogger(BatchingStateChangeStore.class);

    private final ScheduledExecutorService executorService;
    private final long scheduleDelayMs;
    private final BlockingQueue<StateChangeSet> scheduled;
    private final AtomicBoolean queueDrainPending;
    private final RetryPolicy retryPolicy;
    private volatile Throwable error;
    private final int sizeThreshold;
    private final RetryingExecutor retryingExecutor;
    private final StateChangeStore delegate;

    BatchingStateChangeStore(
            long persistDelayMs,
            int sizeThreshold,
            int requestQueueCapacity,
            int threadPoolCoreSize,
            RetryPolicy retryPolicy,
            StateChangeFsStore delegate) {
        this.scheduleDelayMs = persistDelayMs;
        // todo: use non-fair queue?
        this.scheduled = new ArrayBlockingQueue<>(requestQueueCapacity, true);
        // todo: setup error handler?
        // todo: shutdown executor
        this.executorService = Executors.newScheduledThreadPool(1);
        this.queueDrainPending = new AtomicBoolean(false);
        this.retryPolicy = retryPolicy;
        // todo: use single executor?
        this.retryingExecutor = new RetryingExecutor(threadPoolCoreSize);
        this.sizeThreshold = sizeThreshold;
        this.delegate = delegate;
    }

    @Override
    public void save(Collection<StateChangeSet> changeSets) {
        if (error != null) {
            LOG.debug("don't persist {} changesets, already failed", changeSets.size());
            changeSets.forEach(cs -> cs.failed(error));
            return;
        }
        LOG.debug("persist {} changeSets", changeSets.size());
        try {
            scheduled.addAll(changeSets);
            scheduleUploadIfNeeded();
        } catch (Exception e) {
            changeSets.forEach(cs -> cs.failed(e));
            throw new RuntimeException(e);
        }
    }

    private void scheduleUploadIfNeeded() {
        // at most 1 scheduled upload at a time
        if (queueDrainPending.compareAndSet(false, true)) {
            if (scheduleDelayMs == 0 || scheduled.size() >= sizeThreshold) {
                executorService.submit(this::drainAndSave);
            } else {
                executorService.schedule(this::drainAndSave, scheduleDelayMs, MILLISECONDS);
            }
        }
    }

    private void drainAndSave() {
        // todo: check error?
        Collection<StateChangeSet> changeSets = new ArrayList<>();
        scheduled.drainTo(changeSets);
        try {
            queueDrainPending.set(false); // allow other uploads to be scheduled and run
            if (!scheduled.isEmpty()) {
                scheduleUploadIfNeeded(); // re-schedule in case if other thread added changes which
                // weren't drained nor scheduled for upload
            }
            retryingExecutor.execute(retryPolicy, () -> delegate.save(changeSets));
        } catch (Throwable t) {
            changeSets.forEach(changeSet -> changeSet.failed(t));
            if (t instanceof IOException) {
                // todo: search for IOException in exception chain?
                LOG.warn("Caught IO exception while uploading", t);
            } else {
                // todo: handle error using executor methods?
                // todo: shutdown executor?
                error = t;
                ExceptionUtils.rethrow(t);
            }
        }
    }
}
