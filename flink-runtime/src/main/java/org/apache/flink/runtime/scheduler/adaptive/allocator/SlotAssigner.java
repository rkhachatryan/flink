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
import org.apache.flink.runtime.jobmaster.SlotInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/** Interface for assigning slots to slot sharing groups. */
@Internal
public interface SlotAssigner {

    AssignmentResult assignSlots(
            Collection<? extends SlotInfo> slots,
            Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup> groups);

    class AssignmentResult {
        public final List<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> assignments;
        public final Collection<? extends SlotInfo> remainingSlots;

        public AssignmentResult(
                List<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> assignments,
                Collection<? extends SlotInfo> remainingSlots) {
            this.assignments = assignments;
            this.remainingSlots = remainingSlots;
        }

        public static AssignmentResult of(
                List<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> assigned,
                Iterator<? extends SlotInfo> remainingIterator) {
            List<SlotInfo> list = new ArrayList<>();
            remainingIterator.forEachRemaining(list::add);
            return new AssignmentResult(assigned, list);
        }
    }
}
