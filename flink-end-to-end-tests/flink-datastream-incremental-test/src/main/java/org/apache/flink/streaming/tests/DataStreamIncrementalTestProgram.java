/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.tests.DataStreamIncrementalTestJobFactory.setupEnvironment;

/**
 * Test program for incremental file-system backend.
 */
public class DataStreamIncrementalTestProgram {

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		setupEnvironment(env, pt);

		int stateSize = pt.getInt("state_size_elements", 10_000); // total state size in elements
		int stateTtl = pt.getInt("state_ttl", Integer.MAX_VALUE);
		int payloadLength = pt.getInt("payload_size_bytes", 10_000); // size of payload in a single element
		int serializerComputeIterations = pt.getInt("serialize_compute_iterations", 50_000); // emulate cpu-intensive serialization
		int sleepPerElementMs = pt.getInt("src_sleep_per_element_ms", 1);

		env
			.addSource(new TestEventSource(stateSize, sleepPerElementMs, payloadLength))
			.keyBy((KeySelector<TestEvent, Integer>) value -> value.id)
			.addSink(new IncrementalSink(stateTtl, serializerComputeIterations));
		env.execute();
	}

}
