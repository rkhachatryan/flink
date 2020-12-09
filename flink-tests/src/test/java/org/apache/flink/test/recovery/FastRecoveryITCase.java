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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * fixme.
 */
public class FastRecoveryITCase extends TestLogger {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

	@Test
	public void testNoStateDownload() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.newFolder().getAbsolutePath());
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);
		env.setParallelism(1);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0));
		env
			.fromCollection(LongStream.range(0, 1000).boxed().collect(Collectors.toList()))
			.map(new LongLongMapFunction())
			.addSink(new DiscardingSink<>());
//			.addSink(new PrintSinkFunction<>());
		env.execute();
	}

	private static class LongLongMapFunction implements MapFunction<Long, Long>, CheckpointedFunction {
		private ListState<String> state;
		private int numProcessed = 0;

		@Override
		public Long map(Long value) throws Exception {
			Thread.sleep(1);
			if (numProcessed >= 10 && new Random().nextInt(1000) == 0) {
				System.out.println("fail " + value);
				throw new RuntimeException("expected test failure");
			}
			numProcessed++;
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.add("asd");
			System.out.println("snapshotState");
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("test", String.class));
			numProcessed = 0;
			System.out.println("initializeState");
		}
	}
}
