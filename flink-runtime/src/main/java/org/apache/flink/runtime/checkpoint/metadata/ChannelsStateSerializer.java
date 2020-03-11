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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.runtime.checkpoint.SubtaskChannelsState;
import org.apache.flink.runtime.checkpoint.TaskChannelsState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.ChannelStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.util.Preconditions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ChannelsStateSerializer {

	private enum HandleSerializer {
		INPUT_CHANNEL_STATE(0, InputChannelStateHandle.class) {
			@Override
			public void serialize(DataOutputStream dos, ChannelStateHandle h0) throws IOException {
				final InputChannelStateHandle h = (InputChannelStateHandle) h0;
				dos.writeInt(h.getInputChannelInfo().getGateIdx());
				dos.writeInt(h.getInputChannelInfo().getInputChannelIdx());
				// todo: serialize specific fields (delegate stream handle) when available in code
			}

			@Override
			public ChannelStateHandle deserialize(DataInputStream dis) throws IOException {
				final int gateIdx = dis.readInt();
				final int inputChannelIdx = dis.readInt();
				throw new RuntimeException("ChannelStateHandle deserialize not implemented"); // fixme
			}
		},
		RESULT_SUBPARTITION(1, ResultSubpartitionStateHandle.class) {
			@Override
			public void serialize(DataOutputStream dos, ChannelStateHandle h0) throws IOException {
				final ResultSubpartitionStateHandle h = (ResultSubpartitionStateHandle) h0;
				dos.writeInt(h.getResultSubpartitionInfo().getPartitionIdx());
				dos.writeInt(h.getResultSubpartitionInfo().getSubPartitionIdx());
				// todo: serialize specific fields (delegate stream handle) when available in code
			}

			@Override
			public ChannelStateHandle deserialize(DataInputStream dis) throws IOException {
				final int partitionIdx = dis.readInt();
				final int subPartitionIdx = dis.readInt();
				throw new RuntimeException("ChannelStateHandle deserialize not implemented"); // fixme
			}
		};

		private final int code;
		private final Class<? extends ChannelStateHandle> clazz;

		HandleSerializer(int code, Class<? extends ChannelStateHandle> clazz) {
			this.code = code;
			this.clazz = clazz;
		}

		public abstract void serialize(DataOutputStream dos, ChannelStateHandle h0) throws IOException;

		public abstract ChannelStateHandle deserialize(DataInputStream dis) throws IOException;

		private static Map<Integer, HandleSerializer> codeMap = new HashMap<>();
		private static Map<Class<?>, HandleSerializer> classMap = new HashMap<>();

		static {
			for (HandleSerializer v : values()) {
				Preconditions.checkState(!codeMap.containsKey(v.code));
				Preconditions.checkState(!classMap.containsKey(v.clazz));
				codeMap.put(v.code, v);
				classMap.put(v.clazz, v);
			}
		}

		public static HandleSerializer byCode(int code) {
			final HandleSerializer result = codeMap.get(code);
			Preconditions.checkNotNull(result, "channel state handle of unknown type: " + code);
			return result;
		}

		public static HandleSerializer byClass(Class<? extends ChannelStateHandle> clazz) {
			final HandleSerializer result = classMap.get(clazz);
			Preconditions.checkNotNull(result, "channel state handle of unknown type: " + clazz);
			return result;
		}
	}

	void serializeChannelsState(CheckpointMetadata checkpointMetadata, DataOutputStream dos) throws IOException {
		Collection<TaskChannelsState> channelsStates = checkpointMetadata.getChannelsStates();
		dos.writeInt(channelsStates.size());
		for (TaskChannelsState subtaskChannelsState : channelsStates) {
			serializeTaskChannelsState(subtaskChannelsState, dos);
		}
	}

	private void serializeTaskChannelsState(TaskChannelsState taskChannelsState, DataOutputStream dos) throws IOException {
		dos.writeLong(taskChannelsState.getOperatorID().getLowerPart());
		dos.writeLong(taskChannelsState.getOperatorID().getUpperPart());
		dos.writeInt(taskChannelsState.getSubtaskChannelsStates().size());
		for (Map.Entry<Integer, SubtaskChannelsState> e : taskChannelsState.getSubtaskChannelsStates().entrySet()) {
			dos.writeInt(e.getKey());
			serializeSubtaskChannelsState(e.getValue(), dos);
		}
	}

	private void serializeSubtaskChannelsState(SubtaskChannelsState subtaskChannelsState, DataOutputStream dos) throws IOException {
		dos.writeInt(subtaskChannelsState.getStateHandles().size());
		for (ChannelStateHandle h : subtaskChannelsState.getStateHandles()) {
			HandleSerializer.byClass(h.getClass()).serialize(dos, h);
		}
	}

	List<TaskChannelsState> deserializeTaskChannelsStates(DataInputStream dis) throws IOException {
		final int count = dis.readInt();
		final List<TaskChannelsState> channelsStates = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			channelsStates.add(deserializeTaskChannelsState(dis));
		}
		return channelsStates;
	}

	private TaskChannelsState deserializeTaskChannelsState(DataInputStream dis) throws IOException {
		final long lower = dis.readLong();
		final long upper = dis.readLong();
		final int count = dis.readInt();
		HashMap<Integer, SubtaskChannelsState> subtaskChannelsStates = new HashMap<>(count);
		for (int i = 0; i < count; i++) {
			subtaskChannelsStates.put(dis.readInt(), deserializeSubtaskChannelsState(dis));
		}
		return new TaskChannelsState(new OperatorID(lower, upper), subtaskChannelsStates);
	}

	private SubtaskChannelsState deserializeSubtaskChannelsState(DataInputStream dis) throws IOException {
		ArrayList<ChannelStateHandle> handles = new ArrayList<>();
		int count = dis.readInt();
		for (int i = 0; i < count; i++) {
			handles.add(HandleSerializer.byCode(dis.readUnsignedShort()).deserialize(dis));
		}
		return new SubtaskChannelsState(handles);
	}

}
