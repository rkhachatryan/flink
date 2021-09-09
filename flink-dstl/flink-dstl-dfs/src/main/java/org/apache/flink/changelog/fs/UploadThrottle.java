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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/** Helper class to throttle upload requests when the in-flight data size limit is exceeded. */
@ThreadSafe
class UploadThrottle {
    private static final Logger LOG = LoggerFactory.getLogger(UploadThrottle.class);

    private final Object lock = new Object();
    private final long maxBytesInFlight;

    @GuardedBy("lock")
    private long inFlightBytesCounter = 0;

    UploadThrottle(long maxBytesInFlight) {
        this.maxBytesInFlight = maxBytesInFlight;
    }

    /**
     * Seize <b>bytes</b> capacity, waiting if needed. Called by the Task thread.
     *
     * @throws InterruptedException
     */
    public void seizeCapacity(long bytes) throws InterruptedException {
        synchronized (lock) {
            while (inFlightBytesCounter > maxBytesInFlight) {
                LOG.info("In flight data size threshold exceeded: {}", maxBytesInFlight);
                lock.wait();
            }
            inFlightBytesCounter += bytes;
        }
    }

    /**
     * Release capacity, signalling waiting threads, if any. Called by {@link
     * BatchingStateChangeUploader} (IO thread).
     */
    public void releaseCapacity(long bytes) {
        synchronized (lock) {
            inFlightBytesCounter -= bytes;
            lock.notifyAll();
        }
    }
}
