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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * State of channels of a single task ({@link org.apache.flink.runtime.executiongraph.ExecutionJobVertex}).
 */
public class TaskChannelsState implements CompositeStateHandle {
	private static final long serialVersionUID = 1L;

	// todo: consider adding parallelism and max parallelism
	// task.tail.operator.id
	private final OperatorID operatorID;
	// subtask index -> channels state
	private final Map<Integer, SubtaskChannelsState> subtaskChannelsStates;

	public TaskChannelsState(OperatorID operatorID) {
		this(operatorID, new HashMap<>());
	}

	public TaskChannelsState(OperatorID operatorID, Map<Integer, SubtaskChannelsState> subtaskChannelsStates) {
		this.operatorID = checkNotNull(operatorID);
		this.subtaskChannelsStates = checkNotNull(subtaskChannelsStates);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final TaskChannelsState that = (TaskChannelsState) o;
		return operatorID.equals(that.operatorID) && subtaskChannelsStates.equals(that.subtaskChannelsStates);
	}

	@Override
	public int hashCode() {
		return Objects.hash(operatorID, subtaskChannelsStates);
	}

	@Override
	public String toString() {
		return "TaskChannelsState{" + "operatorID=" + operatorID + ", subtaskChannelsStates=" + subtaskChannelsStates + '}';
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		for (SubtaskChannelsState state : subtaskChannelsStates.values()) {
			state.registerSharedStates(stateRegistry);
		}
	}

	@Override
	public void discardState() throws Exception {
		for (SubtaskChannelsState state : subtaskChannelsStates.values()) {
			state.discardState();
		}
	}

	@Override
	public long getStateSize() {
		int size = 0;
		for (SubtaskChannelsState state : subtaskChannelsStates.values()) {
			size += state.getStateSize();
		}
		return size;
	}

	public void putSubtaskState(int subtaskIdx, SubtaskChannelsState subtaskState) {
		checkArgument(!subtaskChannelsStates.containsKey(subtaskIdx), "state for subtask index " + subtaskIdx + " already added");
		checkNotNull(subtaskState, "state for subtask index " + subtaskIdx + " null");
		subtaskChannelsStates.put(subtaskIdx, subtaskState);
	}

	public OperatorID getOperatorID() {
		return operatorID;
	}

	public Map<Integer, SubtaskChannelsState> getSubtaskChannelsStates() {
		return subtaskChannelsStates;
	}
}
