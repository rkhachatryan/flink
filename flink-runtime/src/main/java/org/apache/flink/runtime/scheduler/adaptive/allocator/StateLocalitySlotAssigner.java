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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.DefaultSlotAssigner.createExecutionSlotSharingGroups;

/** A {@link SlotAssigner} that assigns slots based on the number of local key groups. */
@Internal
public class StateLocalitySlotAssigner implements SlotAssigner {

    private static class AllocationScore implements Comparable<AllocationScore> {

        private final String group;
        private final AllocationID allocationId;

        public AllocationScore(String group, AllocationID allocationId, int score) {
            this.group = group;
            this.allocationId = allocationId;
            this.score = score;
        }

        private final int score;

        public String getGroup() {
            return group;
        }

        public AllocationID getAllocationId() {
            return allocationId;
        }

        public int getScore() {
            return score;
        }

        @Override
        public int compareTo(StateLocalitySlotAssigner.AllocationScore other) {
            int result = Integer.compare(score, other.score);
            if (result != 0) {
                return result;
            }
            result = other.allocationId.compareTo(allocationId);
            if (result != 0) {
                return result;
            }
            return other.group.compareTo(group);
        }
    }

    private final Map<AllocationID, Map<JobVertexID, KeyGroupRange>> locality;

    public StateLocalitySlotAssigner(ExecutionGraph archivedExecutionGraph) {
        this(calculateLocalKeyGroups(archivedExecutionGraph));
    }

    public StateLocalitySlotAssigner(Map<AllocationID, Map<JobVertexID, KeyGroupRange>> locality) {
        this.locality = locality;
    }

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism) {
        final List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }
        final Map<JobVertexID, Integer> parallelism = new HashMap<>();
        allGroups.forEach(
                group ->
                        group.getContainedExecutionVertices()
                                .forEach(
                                        evi ->
                                                parallelism.merge(
                                                        evi.getJobVertexId(), 1, Integer::sum)));

        final PriorityQueue<AllocationScore> scores =
                new PriorityQueue<>(Comparator.reverseOrder());
        for (ExecutionSlotSharingGroup group : allGroups) {
            calculateScore(group, parallelism, jobInformation)
                    .forEach(
                            (allocationId, score) ->
                                    scores.add(
                                            new AllocationScore(
                                                    group.getId(), allocationId, score)));
        }

        Map<String, ExecutionSlotSharingGroup> groupsById =
                allGroups.stream().collect(toMap(ExecutionSlotSharingGroup::getId, identity()));
        Map<AllocationID, SlotInfo> slotsById =
                freeSlots.stream().collect(toMap(SlotInfo::getAllocationId, identity()));
        AllocationScore score;
        final Collection<SlotAssignment> assignments = new ArrayList<>();
        while ((score = scores.poll()) != null) {
            SlotInfo slot = slotsById.remove(score.getAllocationId());
            if (slot != null) {
                ExecutionSlotSharingGroup group = groupsById.remove(score.getGroup());
                if (group != null) {
                    assignments.add(new SlotAssignment(slot, group));
                }
            }
        }
        // Distribute the remaining slots with no score
        Iterator<? extends SlotInfo> remainingSlots = slotsById.values().iterator();
        for (ExecutionSlotSharingGroup group : groupsById.values()) {
            assignments.add(new SlotAssignment(remainingSlots.next(), group));
            remainingSlots.remove();
        }

        return assignments;
    }

    public Map<AllocationID, Integer> calculateScore(
            ExecutionSlotSharingGroup group,
            Map<JobVertexID, Integer> parallelism,
            JobInformation jobInformation) {
        final Map<AllocationID, Integer> score = new HashMap<>();
        for (ExecutionVertexID evi : group.getContainedExecutionVertices()) {
            final KeyGroupRange kgr =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            jobInformation
                                    .getVertexInformation(evi.getJobVertexId())
                                    .getMaxParallelism(),
                            parallelism.get(evi.getJobVertexId()),
                            evi.getSubtaskIndex());
            locality.forEach(
                    (allocationId, potentials) -> {
                        KeyGroupRange prev = potentials.get(evi.getJobVertexId());
                        if (prev != null) {
                            int intersection = prev.getIntersection(kgr).getNumberOfKeyGroups();
                            if (intersection > 0) {
                                score.merge(allocationId, intersection, Integer::sum);
                            }
                        }
                    });
        }
        return score;
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
}
