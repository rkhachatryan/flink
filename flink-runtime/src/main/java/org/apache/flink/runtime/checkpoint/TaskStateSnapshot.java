/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class encapsulates state handles to the snapshots of all operator instances executed within one task. A task
 * can run multiple operator instances as a result of operator chaining, and all operator instances from the chain can
 * register their state under their operator id. Each operator instance is a physical execution responsible for
 * processing a partition of the data that goes through a logical operator. This partitioning happens to parallelize
 * execution of logical operators, e.g. distributing a map function.
 *
 * <p>One instance of this class contains the information that one task will send to acknowledge a checkpoint request by
 * the checkpoint coordinator. Tasks run operator instances in parallel, so the union of all
 * {@link TaskStateSnapshot} that are collected by the checkpoint coordinator from all tasks represent the whole
 * state of a job at the time of the checkpoint.
 *
 * <p>This class should be called TaskState once the old class with this name that we keep for backwards
 * compatibility goes away.
 */
public class TaskStateSnapshot implements CompositeStateHandle {

	public static final TaskStateSnapshot EMPTY = new TaskStateSnapshot(Collections.emptyMap(), SubtaskChannelsState.EMPTY);

	private static final long serialVersionUID = 1L;

	/**
	 * Mapping from an operator id to the state of one subtask of this operator.
	 */
	private final Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID;

	private final SubtaskChannelsState subtaskChannelsState;

	public TaskStateSnapshot(OperatorID id, OperatorSubtaskState state) {
		this(Collections.singletonMap(id, state), SubtaskChannelsState.EMPTY);
	}

	public TaskStateSnapshot(Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID) {
		this(subtaskStatesByOperatorID, SubtaskChannelsState.EMPTY);
	}

	public TaskStateSnapshot(Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID, SubtaskChannelsState subtaskChannelsState) {
		this.subtaskStatesByOperatorID = Preconditions.checkNotNull(Collections.unmodifiableMap(new HashMap<>(subtaskStatesByOperatorID)));
		this.subtaskStatesByOperatorID.values().forEach(Preconditions::checkNotNull);
		this.subtaskChannelsState = Preconditions.checkNotNull(subtaskChannelsState);
	}

	/**
	 * Returns the subtask state for the given operator id (or null if not contained).
	 */
	@Nullable
	public OperatorSubtaskState getSubtaskStateByOperatorID(OperatorID operatorID) {
		return subtaskStatesByOperatorID.get(operatorID);
	}

	/**
	 * Returns the set of all mappings from operator id to the corresponding subtask state.
	 */
	public Map<OperatorID, OperatorSubtaskState> getSubtaskStateMappings() {
		return subtaskStatesByOperatorID;
	}

	/**
	 * Returns true if at least one {@link OperatorSubtaskState} in subtaskStatesByOperatorID has state.
	 */
	public boolean hasState() {
		for (OperatorSubtaskState operatorSubtaskState : subtaskStatesByOperatorID.values()) {
			if (operatorSubtaskState != null && operatorSubtaskState.hasState()) {
				return true;
			}
		}
		return subtaskChannelsState.hasState();
	}

	@Override
	public void discardState() throws Exception {
		StateUtil.bestEffortDiscardAllStateObjects(subtaskStatesByOperatorID.values());
		StateUtil.bestEffortDiscardAllStateObjects(subtaskChannelsState.getStateHandles());
	}

	@Override
	public long getStateSize() {
		long size = 0L;

		for (OperatorSubtaskState subtaskState : subtaskStatesByOperatorID.values()) {
			if (subtaskState != null) {
				size += subtaskState.getStateSize();
			}
		}
		size += subtaskChannelsState.getStateSize();

		return size;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		for (OperatorSubtaskState operatorSubtaskState : subtaskStatesByOperatorID.values()) {
			if (operatorSubtaskState != null) {
				operatorSubtaskState.registerSharedStates(stateRegistry);
			}
		}
		subtaskChannelsState.registerSharedStates(stateRegistry);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TaskStateSnapshot that = (TaskStateSnapshot) o;

		return subtaskStatesByOperatorID.equals(that.subtaskStatesByOperatorID) &&
			subtaskChannelsState.equals(that.subtaskChannelsState);
	}

	@Override
	public int hashCode() {
		return Objects.hash(subtaskStatesByOperatorID, subtaskChannelsState);
	}

	@Override
	public String toString() {
		return "TaskOperatorSubtaskStates{" +
			"subtaskStatesByOperatorID=" + subtaskStatesByOperatorID +
			"channelsState=" + subtaskChannelsState +
			'}';
	}

	public SubtaskChannelsState getSubtaskChannelsState() {
		return subtaskChannelsState;
	}

}
