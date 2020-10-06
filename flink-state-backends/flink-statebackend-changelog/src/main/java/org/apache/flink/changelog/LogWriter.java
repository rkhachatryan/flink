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

package org.apache.flink.changelog;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.CompletableFuture;

/** Allows to write data to the log. Scoped to a single writer (e.g. state backend). */
@Internal
public interface LogWriter extends AutoCloseable {

    /** Unique ID of this log. */
    LogId logId();

    /** Appends the provided data to this log. No persistency guarantees. */
    void append(int keyGroup, byte[] key, byte[] value, long timestamp);

    /**
     * Get {@link SequenceNumber} of the last element added by {@link #append(int, byte[], byte[],
     * long) append}.
     */
    SequenceNumber lastAppendedSqn();

    /**
     * Durably store previously {@link #append(int, byte[], byte[], long) appended} data.
     *
     * @param after inclusive
     * @param until exclusive
     */
    CompletableFuture<LogPointer> persistUntil(SequenceNumber after, SequenceNumber until);

    /**
     * Close this log. No new appends will be possible. Any appended but not persisted records will
     * be lost.
     */
    void close();
}
