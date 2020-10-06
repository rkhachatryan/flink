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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link LogId} and some backend-specific data associated with this log. The data can be used by
 * implementations to identify the log along with {@link LogId}.
 */
@Internal
public interface LogPointer {

    LogId logId();

    byte[] passThroughData();

    /** Generic {@link LogPointer}. */
    class GenericLogPointer implements LogPointer {
        private final LogId logId;
        private final byte[] passThroughData;

        GenericLogPointer(LogId logId, byte[] passThroughData) {
            this.logId = checkNotNull(logId);
            this.passThroughData = checkNotNull(passThroughData);
        }

        @Override
        public LogId logId() {
            return logId;
        }

        @Override
        public byte[] passThroughData() {
            return passThroughData;
        }

        @Override
        public String toString() {
            return String.format("logId=%s, passThroughDataLen=%s", logId, passThroughData.length);
        }
    }

    static LogPointer of(LogId logId) {
        return of(logId, new byte[0]);
    }

    static LogPointer of(LogId logId, byte[] passThroughData) {
        return new GenericLogPointer(logId, passThroughData);
    }
}
