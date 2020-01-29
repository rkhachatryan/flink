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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects.JDBCDialectName;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.stream.Stream;

/**
 * JDBC sink DML options.
 */
public class JdbcDmlOptions extends JdbcTypedQueryOptions {

	private static final long serialVersionUID = 1L;

	private final String[] fieldNames;
	@Nullable
	private final String[] keyFields;
	private final String tableName;
	private final JDBCDialectName dialectName;
	@Nullable
	private final JDBCDialect customDialect;

	public static JdbcDmlOptionsBuilder builder() {
		return new JdbcDmlOptionsBuilder();
	}

	private JdbcDmlOptions(String tableName, JDBCDialectName dialectName, String[] fieldNames, int[] fieldTypes, String[] keyFields, @Nullable JDBCDialect customDialect) {
		super(fieldTypes);
		this.tableName = Preconditions.checkNotNull(tableName, "table is empty");
		this.dialectName = Preconditions.checkNotNull(dialectName, "dialect name is empty");
		this.fieldNames = Preconditions.checkNotNull(fieldNames, "field names is empty");
		this.keyFields = keyFields;
		this.customDialect = customDialect;
		Preconditions.checkArgument((dialectName == JDBCDialectName.CUSTOM) == (customDialect != null));
	}

	public String getTableName() {
		return tableName;
	}

	public JDBCDialectName getDialectName() {
		Preconditions.checkNotNull(dialectName, "dialect not set");
		return dialectName;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public String[] getKeyFields() {
		return keyFields;
	}

	@Nullable
	public JDBCDialect getCustomDialect() {
		return customDialect;
	}

	/**
	 * JDBCUpsertOptionsBuilder.
	 */
	public static class JdbcDmlOptionsBuilder extends JDBCUpdateQueryOptionsBuilder<JdbcDmlOptionsBuilder> {
		private String tableName;
		private String[] fieldNames;
		private String[] keyFields;
		private JDBCDialectName dialectName;
		private JDBCDialect customDialect;

		@Override
		protected JdbcDmlOptionsBuilder self() {
			return this;
		}

		public JdbcDmlOptionsBuilder withFieldNames(String field, String... fieldNames) {
			this.fieldNames = concat(field, fieldNames);
			return this;
		}

		public JdbcDmlOptionsBuilder withFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		public JdbcDmlOptionsBuilder withKeyFields(String keyField, String... keyFields) {
			this.keyFields = concat(keyField, keyFields);
			return this;
		}

		public JdbcDmlOptionsBuilder withKeyFields(String[] keyFields) {
			this.keyFields = keyFields;
			return this;
		}

		public JdbcDmlOptionsBuilder withTableName(String tableName) {
			this.tableName = tableName;
			return self();
		}

		public JdbcDmlOptionsBuilder withDialect(JDBCDialectName dialect) {
			this.dialectName = dialect;
			return self();
		}

		public void withCustomDialect(JDBCDialect customDialect) {
			this.customDialect = customDialect;
			this.dialectName = JDBCDialectName.CUSTOM;
		}

		public JdbcDmlOptions build() {
			return new JdbcDmlOptions(tableName, dialectName, fieldNames, fieldTypes, keyFields, customDialect);
		}

		static String[] concat(String first, String... next) {
			if (next == null || next.length == 0) {
				return new String[]{first};
			} else {
				return Stream.concat(Stream.of(new String[]{first}), Stream.of(next)).toArray(String[]::new);
			}
		}

	}
}
