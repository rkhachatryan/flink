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

package org.apache.flink.api.java.io.jdbc.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.BiConsumerWithException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

/**
 * Sets {@link PreparedStatement} parameters to use in JDBC Sink based on a specific type of StreamRecord.
 * @param <T> type of payload in {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord StreamRecord}
 * @see JdbcBatchStatementExecutor
 */
@Internal
public interface ParameterSetter<T> extends BiConsumerWithException<PreparedStatement, T, SQLException> {

	/**
	 * Creates a {@link ParameterSetter} for {@link Row} using the provided SQL types array.
	 * Uses {@link org.apache.flink.api.java.io.jdbc.JDBCUtils#setRecordToStatement}
	 */
	static ParameterSetter<Row> forRow(int[] types) {
		return (st, record) -> setRecordToStatement(st, types, record);
	}
}
