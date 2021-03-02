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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

/** {@link ConfigOptions} for {@link FsStateChangelogWriterFactory}. */
@Experimental
@Documentation.ExcludeFromDocumentation
// todo: add descriptions
public class FsStateChangelogOptions {
    public static final ConfigOption<String> BASE_PATH =
            ConfigOptions.key("dstl.dfs.base-path").stringType().noDefaultValue();
    public static final ConfigOption<Boolean> BATCH_ENABLED =
            ConfigOptions.key("dstl.dfs.batch.enabled").booleanType().defaultValue(false);
    public static final ConfigOption<Boolean> COMPRESSION_ENABLED =
            ConfigOptions.key("dstl.dfs.compression.enabled").booleanType().defaultValue(false);
    public static final ConfigOption<MemorySize> APPEND_PERSIST_THRESHOLD =
            ConfigOptions.key("dstl.dfs.append-persist-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("5Mb"));
    public static final ConfigOption<Duration> PERSIST_DELAY =
            ConfigOptions.key("dstl.dfs.batch.persist-delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(10));
    public static final ConfigOption<MemorySize> PERSIST_SIZE_THRESHOLD =
            ConfigOptions.key("dstl.dfs.batch.persist-size-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("10Mb"));
    public static final ConfigOption<String> RETRY_POLICY_NAME =
            ConfigOptions.key("dstl.dfs.retry.policy-name")
                    .stringType()
                    .defaultValue("fixed")
                    .withDescription("valid values: fixed, none");
    public static final ConfigOption<Duration> RETRY_TIMEOUT =
            ConfigOptions.key("dstl.dfs.retry.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1));
    public static final ConfigOption<Integer> RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("dstl.dfs.retry.max-attempts").intType().defaultValue(3);
    public static final ConfigOption<Duration> RETRY_DELAY_AFTER_FAILURE =
            ConfigOptions.key("dstl.dfs.retry.delay-after-failure")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500));
}
