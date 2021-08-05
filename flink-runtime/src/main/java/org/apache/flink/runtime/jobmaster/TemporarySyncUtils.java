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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.checkpoint.PendingCheckpoint;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** FLINK-23647 demo helper class. */
public class TemporarySyncUtils {
    /** The latest aborted checkpoint. */
    public static final AtomicReference<PendingCheckpoint> ABORTED_CHECKPOINT =
            new AtomicReference<>();
    /** Zeroed when the test lists the files. */
    public static final CountDownLatch FILES_LISTED_LATCH = new CountDownLatch(1);
    /** Zeroed when the cleaner cleans the (aborted) checkpoint. */
    public static final CountDownLatch CHECKPOINTS_CLEANED_LATCH = new CountDownLatch(1);
    /** Zeroed when UnalignedCheckpointStressITCase starts. Use to prevent affecting other tests. */
    public static final AtomicBoolean STRESS_TEST_RUNNING = new AtomicBoolean(false);
}
