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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.changelog.LogClient;
import org.apache.flink.changelog.LogId;
import org.apache.flink.changelog.LogPointer;
import org.apache.flink.changelog.LogRecord;
import org.apache.flink.changelog.SequenceNumber;
import org.apache.flink.changelog.fs.LogRecordStreamIterator.LogRecordFilter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

/** Filesystem-based implementation of {@link LogClient}. */
public class FsLogClient implements LogClient {
    private final FsLogPersister persister;
    private final Path basePath;
    private final FileSystem fileSystem;

    public FsLogClient(
            Path basePath,
            long persistDelayMs,
            int requestQueueCapacity,
            int threadPoolCoreSize,
            RetryPolicy retryPolicy)
            throws IOException {
        this.persister =
                new FsLogPersister(
                        basePath,
                        persistDelayMs,
                        requestQueueCapacity,
                        threadPoolCoreSize,
                        retryPolicy);
        this.basePath = basePath;
        this.fileSystem = basePath.getFileSystem();
    }

    @Override
    public FsStateChangelogWriter createWriter(OperatorID operatorID, KeyGroupRange keyGroupRange) {
        return new FsStateChangelogWriter(persister, LogId.UuidLogId.random());
    }

    @Override
    public CloseableIterator<LogRecord> replay(
            LogPointer logPointer,
            SequenceNumber after,
            SequenceNumber until,
            KeyGroupRange keyGroupRange)
            throws IOException {
        return new LogRecordStreamIterator(
                openInputStream(logPointer),
                new LogRecordFilter(logPointer, after, until, keyGroupRange));
    }

    private FSDataInputStream openInputStream(LogPointer logPointer) throws IOException {
        String fileName = new LogPathSerializer().buildFileName(logPointer.passThroughData());
        Path path = new Path(basePath, fileName);
        return fileSystem.open(path);
    }
}
