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

import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class RetryingExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(RetryingExecutor.class);

    private final ScheduledExecutorService executorService;

    RetryingExecutor(int threadPoolCoreSize) {
        executorService = Executors.newScheduledThreadPool(threadPoolCoreSize);
    }

    void execute(RunnableWithException action, RetryPolicy retryPolicy) throws Exception {
        LOG.debug("execute with retryPolicy: {}", retryPolicy);
        Attempt initial = new Attempt(action, retryPolicy, executorService);
        executorService.submit(initial);
        try {
            initial.awaitCompletion();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    private static final class Attempt implements Runnable {
        private final RunnableWithException runnable;
        private final ScheduledExecutorService executorService;
        private final CompletableFuture<Void> resultFuture;
        private final int current;
        private final RetryPolicy retryPolicy;
        private final AtomicBoolean attemptCompleted = new AtomicBoolean(false);

        Attempt(
                RunnableWithException runnable,
                RetryPolicy retryPolicy,
                ScheduledExecutorService executorService) {
            this(1, new CompletableFuture<>(), runnable, retryPolicy, executorService);
        }

        private Attempt(
                int current,
                CompletableFuture<Void> resultFuture,
                RunnableWithException runnable,
                RetryPolicy retryPolicy,
                ScheduledExecutorService executorService) {
            this.current = current;
            this.resultFuture = resultFuture;
            this.runnable = runnable;
            this.retryPolicy = retryPolicy;
            this.executorService = executorService;
        }

        @Override
        public void run() {
            if (!resultFuture.isDone()) {
                Optional<ScheduledFuture<?>> timeoutFuture = scheduleTimeout();
                try {
                    runnable.run();
                    attemptCompleted.set(true);
                    resultFuture.complete(null); // todo medium: executor for callbacks?
                } catch (Exception e) {
                    handleError(e);
                } finally {
                    timeoutFuture.ifPresent(f -> f.cancel(true));
                }
            }
        }

        private void handleError(Exception e) {
            LOG.trace("execution attempt {} failed: {}", current, e.getMessage());
            if (attemptCompleted.compareAndSet(
                    false,
                    true)) { // prevent completing twice for the same attempt (in case of a timeout
                // and another failure)
                long nextAttemptDelay = retryPolicy.retryAfter(current, e);
                if (nextAttemptDelay == 0L) {
                    executorService.submit(next());
                } else if (nextAttemptDelay > 0L) {
                    executorService.schedule(next(), nextAttemptDelay, MILLISECONDS);
                } else {
                    resultFuture.completeExceptionally(e);
                }
            }
        }

        private Attempt next() {
            return new Attempt(current + 1, resultFuture, runnable, retryPolicy, executorService);
        }

        void awaitCompletion() throws ExecutionException, InterruptedException {
            resultFuture.get();
        }

        private Optional<ScheduledFuture<?>> scheduleTimeout() {
            long timeout = retryPolicy.timeoutFor(current);
            return timeout <= 0
                    ? Optional.empty()
                    : Optional.of(
                            executorService.schedule(
                                    () ->
                                            handleError(
                                                    new TimeoutException(
                                                            String.format(
                                                                    "Attempt %d timed out after %dms",
                                                                    current, timeout))),
                                    timeout,
                                    MILLISECONDS));
        }
    }
}
