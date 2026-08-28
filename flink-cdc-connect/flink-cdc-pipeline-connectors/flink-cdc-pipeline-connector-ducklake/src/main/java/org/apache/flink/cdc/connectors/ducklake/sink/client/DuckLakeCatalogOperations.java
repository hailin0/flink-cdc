/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.ducklake.sink.client;

import org.apache.flink.cdc.common.annotation.Internal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/** Minimal catalog operations required by DuckLake DDL and checkpoint transactions. */
@Internal
public interface DuckLakeCatalogOperations extends AutoCloseable {

    void execute(String sql) throws SQLException;

    <T> List<T> query(String sql, RowMapper<T> rowMapper) throws SQLException;

    default void begin() throws SQLException {
        execute("BEGIN TRANSACTION");
    }

    default void commit() throws SQLException {
        execute("COMMIT");
    }

    default void rollback() throws SQLException {
        execute("ROLLBACK");
    }

    @Override
    void close() throws SQLException;

    /** Maps the current JDBC result row to a value. */
    @FunctionalInterface
    interface RowMapper<T> {
        T map(ResultSet resultSet) throws SQLException;
    }
}
