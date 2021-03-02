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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@ThreadSafe
class FsStateChangelogCleanerImpl implements FsStateChangelogCleaner {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogCleanerImpl.class);
    private static final int MAX_TASKS_PER_THREAD = 100;

    private final ExecutorService executor;

    public FsStateChangelogCleanerImpl(int nThreads) {
        // Use a fixed-size thread pool with a bounded queue so that cleanupAsync back pressures
        // callers if the cleanup doesn't keep up.
        // In all cases except abort this are uploader threads; while on abort this is the task
        // thread.
        this.executor =
                new ThreadPoolExecutor(
                        nThreads,
                        nThreads,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(nThreads * MAX_TASKS_PER_THREAD),
                        (ThreadFactory) Thread::new);
    }

    @Override
    public void cleanupAsync(UploadResult uploadResult) {
        LOG.debug("cleanup async store result: {}", uploadResult);
        executor.execute(
                () -> {
                    try {
                        uploadResult.getStreamStateHandle().discardState();
                    } catch (Exception e) {
                        LOG.warn("unable to discard {}", uploadResult);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
            LOG.warn(
                    "Unable to shutdown executor in 10 minutes. Some state files may left undeleted");
        }
    }
}
