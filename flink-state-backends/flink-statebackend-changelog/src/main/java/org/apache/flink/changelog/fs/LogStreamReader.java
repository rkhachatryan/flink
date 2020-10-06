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
import org.apache.flink.changelog.LogId;
import org.apache.flink.changelog.SequenceNumber;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

class LogStreamReader {

    private final DataInputStream dataInput;

    LogStreamReader(FSDataInputStream is) {
        dataInput = new DataInputStream(is);
    }

    int readCount() throws IOException {
        return dataInput.readInt();
    }

    LogId readLogId() throws IOException {
        byte[] bytes = new byte[dataInput.readInt()];
        dataInput.readFully(bytes);
        return LogId.of(UUID.fromString(new String(bytes, StandardCharsets.UTF_8)));
    }

    int readKeyGroup() throws IOException {
        return dataInput.readInt();
    }

    SequenceNumber readSequenceNumber() throws IOException {
        return SequenceNumber.of(dataInput.readLong());
    }

    byte[] readValue() throws IOException {
        int byteCount = dataInput.readInt();
        byte[] data = new byte[byteCount];
        dataInput.readFully(data);
        return data;
    }
}
