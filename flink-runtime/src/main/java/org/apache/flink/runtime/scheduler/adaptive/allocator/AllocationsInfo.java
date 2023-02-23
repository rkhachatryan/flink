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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

class AllocationsInfo {
    private final Map<AllocationID, Map<JobVertexID, KeyGroupRange>> allocations;

    public AllocationsInfo(Map<AllocationID, Map<JobVertexID, KeyGroupRange>> allocations) {
        this.allocations = allocations;
    }

    public static AllocationsInfo fromGraph(@Nullable ExecutionGraph graph) {
        return graph == null ? empty() : new AllocationsInfo(calculateLocalKeyGroups(graph));
    }

    public Map<AllocationID, Map<JobVertexID, KeyGroupRange>> getAllocations() {
        return allocations;
    }

    private static Map<AllocationID, Map<JobVertexID, KeyGroupRange>> calculateLocalKeyGroups(
            ExecutionGraph archivedExecutionGraph) {
        final Map<AllocationID, Map<JobVertexID, KeyGroupRange>> localKeyGroups = new HashMap<>();
        for (ExecutionJobVertex executionJobVertex :
                archivedExecutionGraph.getVerticesTopologically()) {
            for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
                AllocationID allocationId =
                        executionVertex.getCurrentExecutionAttempt().getAssignedAllocationID();
                KeyGroupRange kgr =
                        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                                executionJobVertex.getMaxParallelism(),
                                executionJobVertex.getParallelism(),
                                executionVertex.getParallelSubtaskIndex());
                KeyGroupRange previous =
                        localKeyGroups
                                .computeIfAbsent(allocationId, ignored -> new HashMap<>())
                                .put(executionJobVertex.getJobVertexId(), kgr);
                Preconditions.checkState(
                        previous == null,
                        "Can only have a single key group range of a vertex per slot");
            }
        }
        return localKeyGroups;
    }

    public static AllocationsInfo empty() {
        return new AllocationsInfo(emptyMap());
    }

    public boolean isEmpty() {
        return allocations.isEmpty();
    }
}
