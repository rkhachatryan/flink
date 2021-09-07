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

import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.state.changelog.ChangelogKeyedStateBackend.MaterializationResult;
import org.apache.flink.state.changelog.ChangelogKeyedStateBackend.MaterializationTask;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class MaterializationManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(MaterializationManager.class);

    /** Async thread pool, to complete async phase of materialization. */
    private final ExecutorService asyncOperationsThreadPool;

    /** scheduled executor, periodically trigger materialization. */
    private final ScheduledExecutorService periodicExecutor;

    private final AsyncExceptionHandler asyncExceptionHandler;

    /** Allowed number of consecutive materialization failures. */
    private final int allowedNumberOfFailures;

    /** Number of consecutive materialization failures. */
    private final AtomicInteger numberOfConsecutiveFailures;

    private final ChangelogKeyedStateBackend<?> keyedStateBackend;

    private final String subtaskName = "todo";

    private final long intervalMs;

    MaterializationManager(
            ExecutorService asyncOperationsThreadPool,
            AsyncExceptionHandler asyncExceptionHandler,
            long periodicMaterializeInterval,
            int allowedNumberOfFailures,
            ChangelogKeyedStateBackend<?> keyedStateBackend) {
        this.asyncOperationsThreadPool = asyncOperationsThreadPool;

        this.asyncExceptionHandler = asyncExceptionHandler;
        this.periodicExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                "periodic-materialization-scheduler-" + subtaskName));
        this.allowedNumberOfFailures = allowedNumberOfFailures;
        this.numberOfConsecutiveFailures = new AtomicInteger(allowedNumberOfFailures);
        this.keyedStateBackend = keyedStateBackend;
        this.intervalMs = periodicMaterializeInterval;
    }

    public void start() {
        // todo: ignore subsequent calls (or inline into constructor)
        scheduleNext();
    }

    private void scheduleNext() {
        this.periodicExecutor.schedule(
                this::triggerMaterialization, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void triggerMaterialization() {
        Optional<MaterializationTask> opt = keyedStateBackend.initMaterialization();
        if (opt.isPresent()) {
            asyncOperationsThreadPool.execute(() -> upload(opt.get()));
        } else {
            scheduleNext();
        }
    }

    private void upload(MaterializationTask task) {

        FileSystemSafetyNet.initializeSafetyNetForThread();
        try {
            FutureUtils.runIfNotDoneAndGet(task.future);

            LOG.debug("Task {} finishes asynchronous part of materialization.", subtaskName);

            SnapshotResult<KeyedStateHandle> snapshotResult = task.future.get();

            keyedStateBackend.completeMaterialization(
                    new MaterializationResult(snapshotResult, task.upTo));

            scheduleNext();

        } catch (Exception e) {
            int retryTime = numberOfConsecutiveFailures.incrementAndGet();

            LOG.info(
                    "Task {} asynchronous part of materialization could not be completed for the {} time.",
                    subtaskName,
                    retryTime,
                    e);

            handleExecutionException(task.future);

            if (retryTime < allowedNumberOfFailures) {
                scheduleNext();
            } else {
                asyncExceptionHandler.handleAsyncException(
                        "Task "
                                + subtaskName
                                + " fails to complete the asynchronous part of materialization",
                        e);
            }

        } finally {
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }
    }

    private void handleExecutionException(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> materializedRunnableFuture) {

        LOG.info("Task {} cleanup asynchronous runnable for materialization.", subtaskName);

        if (materializedRunnableFuture != null) {
            // materialization has started
            if (!materializedRunnableFuture.cancel(true)) {
                try {
                    StateObject stateObject = materializedRunnableFuture.get();
                    if (stateObject != null) {
                        stateObject.discardState();
                    }
                } catch (Exception ex) {
                    LOG.debug(
                            "Task "
                                    + subtaskName
                                    + " cancelled execution of snapshot future runnable. "
                                    + "Cancellation produced the following "
                                    + "exception, which is expected and can be ignored.",
                            ex);
                }
            }
        }
    }

    @Override
    public void close() {
        if (!periodicExecutor.isShutdown()) {
            periodicExecutor.shutdownNow();
        }
    }
}
