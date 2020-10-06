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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.changelog.LogId.UuidLogId;
import org.apache.flink.changelog.SequenceNumber;
import org.apache.flink.changelog.SequenceNumber.GenericSequenceNumber;
import org.apache.flink.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// todo medium: write version/format header
class LogStreamWriter {

    private final FSDataOutputStream os;
    private final DataOutput dataOutput;

    LogStreamWriter(FSDataOutputStream os) {
        this.os = os;
        this.dataOutput = new DataOutputViewStreamWrapper(os);
    }

    void writeLogId(UuidLogId id) throws IOException {
        String str = id.id.toString();
        dataOutput.writeInt(str.length());
        dataOutput.writeBytes(str); // todo low: confirm 1 byte is enough
    }

    void writeFragments(SequenceNumber sequenceNumber, List<LogFragment> fragments)
            throws IOException {
        dataOutput.writeLong(
                ((GenericSequenceNumber) sequenceNumber).number); // todo low: re/move cast
        Map<Integer, List<byte[]>> keyGroups = groupByKeyGroup(fragments);
        dataOutput.writeInt(keyGroups.size());
        for (Map.Entry<Integer, List<byte[]>> e : keyGroups.entrySet()) {
            // order of entries doesn't matter
            // order of list elements does matter
            writeForKeyGroup(e.getKey(), e.getValue());
        }
    }

    private Map<Integer, List<byte[]>> groupByKeyGroup(List<LogFragment> fragments) {
        Map<Integer, List<byte[]>> map = new HashMap<>();
        for (LogFragment logFragment : fragments) {
            map.computeIfAbsent(logFragment.keyGroup, unused -> new ArrayList<>())
                    .add(logFragment.data);
        }
        return map;
    }

    private void writeForKeyGroup(int keyGroup, List<byte[]> data) throws IOException {
        dataOutput.writeInt(keyGroup);
        int len = 0;
        for (byte[] bytes : data) {
            len += bytes.length;
        }
        dataOutput.writeInt(len);
        for (byte[] bytes : data) {
            IOUtils.copyBytes(new ByteArrayInputStream(bytes), os, false);
        }
    }

    void writeCount(int size) throws IOException {
        dataOutput.writeInt(size);
    }
}
