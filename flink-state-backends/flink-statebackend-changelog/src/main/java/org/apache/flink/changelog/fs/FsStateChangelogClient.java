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

import org.apache.flink.changelog.StateChangelogClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateChangelogHandleStreamImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Filesystem-based implementation of {@link StateChangelogClient}. */
public class FsStateChangelogClient
        implements StateChangelogClient<StateChangelogHandleStreamImpl> {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogClient.class);

    /**
     * The log id is only needed on write to separate changes from different backends (i.e.
     * operators) in the resulting file.
     */
    private final AtomicInteger logIdGenerator = new AtomicInteger(0);

    private final StateChangeStore store;

    /**
     * Creates {@link FsStateChangelogClient client} that implements batching and retries and uses
     * {@link StateChangeFsStore} under the hood. todo: javadoc params
     */
    public FsStateChangelogClient(
            Path basePath,
            long persistDelayMs,
            int requestQueueCapacity,
            int threadPoolCoreSize,
            int uploadSizeThreshold,
            RetryPolicy retryPolicy)
            throws IOException {
        this(
                new BatchingStateChangeStore(
                        persistDelayMs,
                        uploadSizeThreshold,
                        requestQueueCapacity,
                        threadPoolCoreSize,
                        retryPolicy,
                        new StateChangeFsStore(basePath, basePath.getFileSystem())));
    }

    /**
     * Creates {@link FsStateChangelogClient client} that uses a simple {@link StateChangeFsStore}
     * (i.e without any batching or retrying other than on FS level).
     */
    public FsStateChangelogClient(Path basePath) throws IOException {
        this(new StateChangeFsStore(basePath, basePath.getFileSystem()));
    }

    /** Creates {@link FsStateChangelogClient client} that uses a given {@link StateChangeStore}. */
    public FsStateChangelogClient(StateChangeStore store) {
        this.store = store;
    }

    @Override
    public FsStateChangelogWriter createWriter(OperatorID operatorID, KeyGroupRange keyGroupRange) {
        UUID logId = new UUID(0, logIdGenerator.getAndIncrement());
        LOG.info("createWriter for operator {}/{}: {}", operatorID, keyGroupRange, logId);
        return new FsStateChangelogWriter(logId, keyGroupRange, store);
    }
}
