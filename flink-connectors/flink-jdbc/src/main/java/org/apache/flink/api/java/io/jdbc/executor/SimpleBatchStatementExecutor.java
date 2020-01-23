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

package org.apache.flink.api.java.io.jdbc.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class SimpleBatchStatementExecutor<T> implements JdbcBatchStatementExecutor<T> {

	private final String sql;
	private final ParameterSetter<T> parameterSetter;

	private transient PreparedStatement st;

	SimpleBatchStatementExecutor(String sql, ParameterSetter<T> parameterSetter) {
		this.sql = sql;
		this.parameterSetter = parameterSetter;
	}

	@Override
	public void open(Connection connection) throws SQLException {
		this.st = connection.prepareStatement(sql);
	}

	@Override
	public void process(T record) throws SQLException {
		parameterSetter.accept(st, record);
		st.addBatch();
	}

	@Override
	public void executeBatch() throws SQLException {
		st.executeBatch();
	}

	@Override
	public void close() throws SQLException {
		if (st != null) {
			st.close();
			st = null;
		}
	}
}
