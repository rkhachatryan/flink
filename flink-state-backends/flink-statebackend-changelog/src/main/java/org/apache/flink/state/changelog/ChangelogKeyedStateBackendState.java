/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;

import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Materialization State, only accessed by task thread. */
public class ChangelogKeyedStateBackendState {
    /** Set initially on restore and later upon materialization. */
    private final List<KeyedStateHandle> materializedSnapshot;

    /** Updated initially on restore and later cleared upon materialization. */
    private final List<ChangelogStateHandle> restoredNonMaterialized;

    /**
     * The {@link SequenceNumber} up to which the state is materialized, exclusive. The log should
     * be truncated accordingly.
     */
    private final SequenceNumber materializedTo;

    public ChangelogKeyedStateBackendState(
            List<KeyedStateHandle> materializedSnapshot,
            List<ChangelogStateHandle> restoredNonMaterialized,
            SequenceNumber materializedTo) {
        this.materializedSnapshot = checkNotNull(unmodifiableList((materializedSnapshot)));
        this.restoredNonMaterialized = checkNotNull(unmodifiableList(restoredNonMaterialized));
        this.materializedTo = materializedTo;
    }

    public List<KeyedStateHandle> getMaterializedSnapshot() {
        return materializedSnapshot;
    }

    public List<ChangelogStateHandle> getRestoredNonMaterialized() {
        return restoredNonMaterialized;
    }

    public SequenceNumber lastMaterializedTo() {
        return materializedTo;
    }
}
