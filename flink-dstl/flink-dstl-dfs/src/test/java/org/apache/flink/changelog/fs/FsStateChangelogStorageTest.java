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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.inmemory.StateChangelogStorageTest;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

import static org.apache.flink.changelog.fs.FsStateChangelogCleaner.NO_OP;

/** {@link FsStateChangelogStorage} test. */
@RunWith(Parameterized.class)
public class FsStateChangelogStorageTest extends StateChangelogStorageTest {
    @Parameterized.Parameter public boolean compression;

    @Parameterized.Parameters(name = "use compression = {0}")
    public static Object[] parameters() {
        return new Object[] {true, false};
    }

    @Override
    protected StateChangelogStorage<?> getFactory() throws IOException {
        return new FsStateChangelogStorage(
                Path.fromLocalFile(temporaryFolder.newFolder()),
                compression,
                1024 * 1024 * 10,
                NO_OP);
    }
}
