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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.LongAdder;

import static org.apache.flink.runtime.metrics.MetricNames.CHANGELOG_STORAGE_ATTEMPTS_PER_UPLOAD;
import static org.apache.flink.runtime.metrics.MetricNames.CHANGELOG_STORAGE_LOGS_PER_UPLOAD;
import static org.apache.flink.runtime.metrics.MetricNames.CHANGELOG_STORAGE_QUEUE_SIZE;
import static org.apache.flink.runtime.metrics.MetricNames.CHANGELOG_STORAGE_UPLOAD_FAILURES;
import static org.apache.flink.runtime.metrics.MetricNames.CHANGELOG_STORAGE_UPLOAD_LATENCIES;
import static org.apache.flink.runtime.metrics.MetricNames.CHANGELOG_STORAGE_UPLOAD_REQUESTS;
import static org.apache.flink.runtime.metrics.MetricNames.CHANGELOG_STORAGE_UPLOAD_SIZES;

/**
 * Metrics related to the Changelog Storage used by the Changelog State Backend. Thread-safe given
 * that the {@link #registerQueueSizeGauge(Gauge) provided} QueueSizeGauge is thread-safe.
 * Thread-safety is required because: a) it is used by multiple uploader threads (or different
 * batching threads); b) the same instance can be used by multiple storage instances (see {@link
 * TaskManagerMetricGroup#changelogStorageMetricGroup}.
 */
@ThreadSafe
public class ChangelogStorageMetricGroup extends ProxyMetricGroup<TaskManagerMetricGroup> {
    private static final int WINDOW_SIZE = 1000;

    private final Counter uploadsCounter;
    private final Counter uploadFailures;
    private final Histogram logsPerUpload;
    private final Histogram uploadSizes;
    private final Histogram uploadLatencies;
    private final Histogram attemptsPerUpload;

    public ChangelogStorageMetricGroup(TaskManagerMetricGroup parent) {
        super(parent);
        this.uploadsCounter = counter(CHANGELOG_STORAGE_UPLOAD_REQUESTS, new ThreadSafeCounter());
        this.logsPerUpload =
                histogram(
                        CHANGELOG_STORAGE_LOGS_PER_UPLOAD,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.attemptsPerUpload =
                histogram(
                        CHANGELOG_STORAGE_ATTEMPTS_PER_UPLOAD,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.uploadSizes =
                histogram(
                        CHANGELOG_STORAGE_UPLOAD_SIZES,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.uploadLatencies =
                histogram(
                        CHANGELOG_STORAGE_UPLOAD_LATENCIES,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.uploadFailures = counter(CHANGELOG_STORAGE_UPLOAD_FAILURES, new ThreadSafeCounter());
    }

    public Counter getUploadsCounter() {
        return uploadsCounter;
    }

    public Gauge<Integer> registerQueueSizeGauge(Gauge<Integer> gauge) {
        return gauge(CHANGELOG_STORAGE_QUEUE_SIZE, gauge);
    }

    public Histogram getAttemptsPerUploadHistogram() {
        return attemptsPerUpload;
    }

    public Histogram getLogsPerUploadHistogram() {
        return logsPerUpload;
    }

    public Counter getUploadFailures() {
        return uploadFailures;
    }

    public Histogram getLogsPerUpload() {
        return logsPerUpload;
    }

    public Histogram getUploadSizes() {
        return uploadSizes;
    }

    public Histogram getUploadLatencies() {
        return uploadLatencies;
    }

    public Histogram getAttemptsPerUpload() {
        return attemptsPerUpload;
    }

    private static class ThreadSafeCounter implements Counter {
        private final LongAdder longAdder = new LongAdder();

        @Override
        public void inc() {
            longAdder.increment();
        }

        @Override
        public void inc(long n) {
            longAdder.add(n);
        }

        @Override
        public void dec() {
            longAdder.decrement();
        }

        @Override
        public void dec(long n) {
            longAdder.add(-n);
        }

        @Override
        public long getCount() {
            return longAdder.longValue();
        }
    }
}
