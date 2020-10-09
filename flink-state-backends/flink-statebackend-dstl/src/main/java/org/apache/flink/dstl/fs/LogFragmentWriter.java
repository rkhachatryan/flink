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

package org.apache.flink.dstl.fs;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.dstl.LogId.UuidLogId;
import org.apache.flink.dstl.SequenceNumber;
import org.apache.flink.dstl.SequenceNumber.GenericSequenceNumber;
import org.apache.flink.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class LogFragmentWriter {
	private final FSDataOutputStream os;
	private final DataOutput dataOutput;

	LogFragmentWriter(FSDataOutputStream os) {
		this.os = os;
		this.dataOutput = new DataOutputViewStreamWrapper(os);
	}

	void writeLogId(UuidLogId id) throws IOException {
		dataOutput.writeBytes(id.toString());
	}

	void writeFragments(SequenceNumber sequenceNumber, List<LogFragment> fragments) throws IOException {
		dataOutput.writeLong(((GenericSequenceNumber) sequenceNumber).number); // todo: re/move cast
		for (Map.Entry<Integer, List<byte[]>> e : groupByKeyGroup(fragments).entrySet()) {
			// order of entries doesn't matter
			// order of list elements does matter
			writeForKeyGroup(e.getKey(), e.getValue());
		}
	}

	private Map<Integer, List<byte[]>> groupByKeyGroup(List<LogFragment> fragments) {
		Map<Integer, List<byte[]>> map = new HashMap<>();
		for (LogFragment logFragment : fragments) {
			map.computeIfAbsent(logFragment.keyGroup, unused -> new ArrayList<>()).add(logFragment.data);
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
}
