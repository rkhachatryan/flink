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

import java.io.IOException;
import java.util.Collection;

// todo: implement CheckpointStreamFactory / CheckpointStorageWorkerView - based store.
// Considerations:
// 0. need for checkpointId in the current API to resolve the location
//   option a: pass checkpointId (race condition?)
//   option b: pass location (race condition?)
//   option c: add FsCheckpointStorageAccess.createSharedStateStream
// 1. different settings for materialized/changelog (e.g. timeouts)
// 2. re-use closeAndGetHandle
// 3. re-use in-memory handles (.metadata)
// 4. handle in-memory handles duplication
interface StateChangeStore {
    void save(Collection<StateChangeSet> changeSets) throws IOException;
}
