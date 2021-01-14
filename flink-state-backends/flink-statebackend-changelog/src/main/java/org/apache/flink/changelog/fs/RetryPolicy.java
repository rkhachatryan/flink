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

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Retry policy to use by the state changelog. Aimed to curb tail latencies. */
@Internal
public interface RetryPolicy {
    /** @return timeout in millis. Zero or negative means no timeout. */
    long timeoutFor(int attempt);

    /**
     * @return delay in millis before the next attempt. Negative means no retry, zero means no
     *     delay.
     */
    long retryAfter(int failedAttempt, Exception exception);

    int maxAttempts();

    RetryPolicy NONE =
            new RetryPolicy() {
                @Override
                public long timeoutFor(int attempt) {
                    return -1L;
                }

                @Override
                public long retryAfter(int failedAttempt, Exception exception) {
                    return -1L;
                }

                @Override
                public int maxAttempts() {
                    return 1;
                }
            };

    static RetryPolicy fixed(int maxAttempts, long timeout, long delayAfterFailure) {
        return new FixedRetryPolicy(maxAttempts, timeout, delayAfterFailure);
    }

    /** {@link RetryPolicy} with fixed timeout, delay and max attempts. */
    class FixedRetryPolicy implements RetryPolicy {

        private final long timeout;
        private final int maxAttempts;
        private final long delayAfterFailure;

        FixedRetryPolicy(int maxAttempts, long timeout, long delayAfterFailure) {
            this.maxAttempts = maxAttempts;
            this.timeout = timeout;
            this.delayAfterFailure = delayAfterFailure;
        }

        @Override
        public long timeoutFor(int attempt) {
            return timeout;
        }

        @Override
        public long retryAfter(int attempt, Exception exception) {
            if (attempt >= maxAttempts) {
                return -1L;
            } else if (exception instanceof TimeoutException) {
                return 0L;
            } else if (exception instanceof IOException) {
                return delayAfterFailure; // todo medium: handle request throttling
            } else {
                return -1L;
            }
        }

        @Override
        public int maxAttempts() {
            return maxAttempts;
        }

        @Override
        public String toString() {
            return "timeout="
                    + timeout
                    + ", maxAttempts="
                    + maxAttempts
                    + ", delay="
                    + delayAfterFailure;
        }
    }
}
