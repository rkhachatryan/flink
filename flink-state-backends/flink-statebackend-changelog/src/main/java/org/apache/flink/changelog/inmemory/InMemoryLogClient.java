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

package org.apache.flink.changelog.inmemory;

import org.apache.flink.changelog.LogClient;
import org.apache.flink.changelog.LogId;
import org.apache.flink.changelog.LogPointer;
import org.apache.flink.changelog.LogRecord;
import org.apache.flink.changelog.SequenceNumber;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.IdentityHashMap;
import java.util.Map;

/** An in-memory (non-production) implementation of {@link LogClient}. */
@NotThreadSafe
public class InMemoryLogClient implements LogClient {

    private final Map<LogId, InMemoryStateChangelogWriter> writers = new IdentityHashMap<>();

    @Override
    public InMemoryStateChangelogWriter createWriter(OperatorID operatorID, KeyGroupRange keyGroupRange) {
        // todo low: validate args
        InMemoryStateChangelogWriter writer = new InMemoryStateChangelogWriter(new LogId() {});
        writers.put(writer.logId(), writer);
        return writer;
    }

    @Override
    public CloseableIterator<LogRecord> replay(
            LogPointer logPointer,
            SequenceNumber after,
            SequenceNumber until,
            KeyGroupRange keyGroupRange) {
        InMemoryStateChangelogWriter writer = writers.get(logPointer.logId());
        Preconditions.checkArgument(writer != null, "Unknown Log: %s", logPointer.logId());
        return writer.replay(after, until, keyGroupRange);
    }
}
