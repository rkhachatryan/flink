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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Random;
import java.util.function.Supplier;

class EventSerializer extends TypeSerializerSingleton<TestEvent> {

	private static final TestEvent ZERO = new TestEvent(0, 0, new byte[0]);
	private final Random random;
	private final int computeIterations;

	EventSerializer(int computeIterations) {
		this.computeIterations = computeIterations;
		this.random = new Random();
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public TestEvent createInstance() {
		return ZERO;
	}

	@Override
	public TestEvent copy(TestEvent from) {
		return new TestEvent(from.id, from.timesSeen, from.payload);
	}

	@Override
	public TestEvent copy(TestEvent from, TestEvent reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(TestEvent record, DataOutputView target) throws IOException {
		target.writeInt(record.id);
		target.writeInt(record.timesSeen);
		target.writeInt(record.payload.length);
		target.write(record.payload);
		target.writeInt(computeFake());
	}

	@Override
	public TestEvent deserialize(DataInputView source) throws IOException {
		int id = source.readInt();
		int timesSeen = source.readInt();
		byte[] payload = new byte[source.readInt()];
		final int bytesRead = source.read(payload);
		Preconditions.checkState(bytesRead == payload.length);
		source.skipBytes(Integer.BYTES);
		return new TestEvent(id, timesSeen, payload);
	}

	@Override
	public TestEvent deserialize(TestEvent reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public TypeSerializerSnapshot<TestEvent> snapshotConfiguration() {
		return new TestEventTypeSerializerSnapshot();
	}

	private int computeFake() {
		int result = 0;
		for (int i = 0; i < computeIterations; i++) {
			result += random.nextInt();
		}
		return result;
	}

	private static class EventSerializerSnapshot extends SimpleTypeSerializerSnapshot<Integer> {

		public EventSerializerSnapshot(Supplier<? extends TypeSerializer<Integer>> serializerSupplier) {
			super(serializerSupplier);
		}
	}

	private static class TestEventTypeSerializerSnapshot implements TypeSerializerSnapshot<TestEvent> {

		private static final int VERSION = 0;
		private int computeIterations;

		@Override
		public int getCurrentVersion() {
			return VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			out.writeInt(computeIterations);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			Preconditions.checkArgument(readVersion == VERSION);
			computeIterations = in.readInt();
		}

		@Override
		public TypeSerializer<TestEvent> restoreSerializer() {
			return new EventSerializer(computeIterations);
		}

		@Override
		public TypeSerializerSchemaCompatibility<TestEvent> resolveSchemaCompatibility(TypeSerializer<TestEvent> newSerializer) {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}
	}
}
