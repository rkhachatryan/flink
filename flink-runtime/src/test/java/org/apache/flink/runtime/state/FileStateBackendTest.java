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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for the keyed state backend and operator state backend, as created by the
 * {@link FsStateBackend}.
 */
@RunWith(Parameterized.class)
public class FileStateBackendTest extends StateBackendTestBase<FsStateBackend> {

	@Parameterized.Parameters(name = "async={0}, incremental={1}")
	public static Object[][] modes() {
		return new Object[][]{
			new Object[]{true, true},
			new Object[]{true, false},
			new Object[]{false, true},
			new Object[]{false, false},
		};
	}

	@Parameterized.Parameter(0)
	public boolean useAsyncMode;

	@Parameterized.Parameter(1)
	public boolean useIncrementalSnapshots;

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected FsStateBackend getStateBackend() throws Exception {
		return new FsStateBackend(tempFolder.newFolder().toURI(), useAsyncMode, useIncrementalSnapshots);
	}

	@Override
	protected boolean isSerializerPresenceRequiredOnRestore() {
		return true;
	}

	// disable these because the verification does not work for this state backend
	@Override
	@Test
	public void testValueStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testListStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testReducingStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testMapStateRestoreWithWrongSerializers() {}

	@Override
	public void testMapState() throws Exception {
		super.testMapState();
	}

	@Ignore
	@Test
	public void testConcurrentMapIfQueryable() throws Exception {
		super.testConcurrentMapIfQueryable();
	}

	@Test
	@Override
	public void smokeTestMapState() throws Exception {
		super.smokeTestMapState();
	}
}

