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

package org.apache.flink.tests;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.changelog.fs.FsStateChangelogOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class ChangelogTestProgram {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf(params));
        int payloadSize = params.getInt("payloadSizeBytes", 10000);
        int eventsPerSecondPerReader = params.getInt("eventsPerSecondPerReader", 1);
        int windowSize = params.getInt("windowSize", 100);
        int windowSlide = params.getInt("windowSlide", 10);
        int keySpaceSize = params.getInt("keySpaceSize", 1000_000);

        addGraph(
                env,
                payloadSize,
                Time.milliseconds(windowSize),
                Time.milliseconds(windowSlide),
                eventsPerSecondPerReader,
                keySpaceSize);
        env.execute();
    }

    @SuppressWarnings("SameParameterValue")
    private static void addGraph(
            StreamExecutionEnvironment env,
            int payloadSize,
            Time windowSize,
            Time windowSlide,
            int eventsPerSecondPerReader,
            int keySpaceSize) {
        SingleOutputStreamOperator<TestEvent> map =
                env.fromSource(
                                new ThrottlingNumberSequenceSource(
                                        0, Long.MAX_VALUE, eventsPerSecondPerReader),
                                WatermarkStrategy.noWatermarks(),
                                "Sequence Source")
                        .map(num -> key(num, keySpaceSize))
                        .keyBy(num -> key(num, keySpaceSize))
                        .map(
                                el -> {
                                    // Thread.sleep(100); // don't block barriers
                                    byte[] bytes = new byte[payloadSize];
                                    ThreadLocalRandom.current().nextBytes(bytes);
                                    return new TestEvent(el, bytes);
                                });
        DataStreamUtils.reinterpretAsKeyedStream(map, e -> key(e.id, keySpaceSize))
                .window(SlidingProcessingTimeWindows.of(windowSize, windowSlide))
                .process(
                        new ProcessWindowFunction<TestEvent, String, Long, TimeWindow>() {
                            @Override
                            public void process(
                                    Long key,
                                    ProcessWindowFunction<TestEvent, String, Long, TimeWindow>
                                                    .Context
                                            context,
                                    Iterable<TestEvent> elements,
                                    Collector<String> out) {}
                        })
                .addSink(new DiscardingSink<>());
    }

    private static long key(Long num, int keySpaceSize) {
        return num % keySpaceSize;
    }

    @Nonnull
    private static Configuration conf(ParameterTool params) {
        Configuration conf = new Configuration();

        // basic setup
        conf.set(CoreOptions.DEFAULT_PARALLELISM, 4);
        conf.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        conf.set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofMillis(10)); // as fast as can
        conf.set(StateChangelogOptions.STATE_CHANGE_LOG_STORAGE, "filesystem");
        //        conf.set(FsStateChangelogOptions.BASE_PATH, "file:///tmp/flink/changelog");
        conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem"); // cluster-level (path too)
        //        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
        // "file:///tmp/flink/checkpoints");
        //        conf.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        conf.set(
                StateBackendOptions.STATE_BACKEND,
                params.getBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.key())
                        ? "org.apache.flink.state.hashmap.IncrementalHashMapStateBackendFactory"
                        : "org.apache.flink.runtime.state.hashmap.HashMapStateBackendFactory");
        //        conf.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        //        conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        conf.set(CheckpointingOptions.LOCAL_RECOVERY, false); // not supported by changelog
        // tune changelog
        conf.set(FsStateChangelogOptions.PREEMPTIVE_PERSIST_THRESHOLD, MemorySize.ofMebiBytes(10));
        conf.set(StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL, Duration.ofMinutes(3));
        // tune flink
        conf.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ofMebiBytes(1));
        conf.set(PipelineOptions.OBJECT_REUSE, true);
        // back-pressure
        // tried: UC, BD, smaller network size - didn't work (UC helps a bit)
        // non-blocking throttling in sources did work
        // conf.set(ENABLE_UNALIGNED, true);
        // conf.set(BUFFER_DEBLOAT_ENABLED, true);
        // conf.set(BUFFER_DEBLOAT_TARGET, Duration.ofMillis(100));
        // conf.set(BUFFER_DEBLOAT_PERIOD, Duration.ofMillis(50));
        // conf.set(BUFFER_DEBLOAT_SAMPLES, 10);
        // conf.set(NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(100));
        // debug
        conf.set(RestOptions.PORT, 8081);
        conf.set(WebOptions.CHECKPOINTS_HISTORY_SIZE, 100); // cluster-level
        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true); // cluster-level
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay"); // debug any failures

        conf.addAll(params.getConfiguration());

        return conf;
    }

    private static final class TestEvent implements Serializable {
        private final long id;

        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final byte[] payload;

        private TestEvent(long id, byte[] payload) {
            this.id = id;
            this.payload = payload;
        }
    }
}
