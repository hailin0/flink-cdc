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

package org.apache.flink.cdc.connectors.ducklake.sink.v2;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.cdc.connectors.ducklake.testutils.JdbcTestUtils.defaultValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeTableBufferTest {

    private static final TableId TABLE_ID = TableId.tableId("sales", "orders");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator GENERATOR =
            new BinaryRecordDataGenerator(SCHEMA.getColumnDataTypes().toArray(new DataType[0]));

    @TempDir Path tempDir;

    @Test
    void keepsOnlyLastAfterImagePerPrimaryKey() throws Exception {
        DuckLakeTableBuffer buffer = newBuffer();
        buffer.append(insert(1L, "a"));
        buffer.append(update(1L, "a", 1L, "b"));

        DuckLakeWriteResult files = flushToFiles(buffer, "last-value");

        assertThat(readDataRows(files)).containsExactly("1,b");
        assertThat(readKeyRows(files)).containsExactly(1L);
    }

    @Test
    void deleteRemovesBufferedAfterImageButKeepsDeleteKey() throws Exception {
        DuckLakeTableBuffer buffer = newBuffer();
        buffer.append(insert(1L, "a"));
        buffer.append(DataChangeEvent.deleteEvent(TABLE_ID, record(1L, "a")));

        DuckLakeWriteResult files = flushToFiles(buffer, "delete");

        assertThat(files.getDataFilePath()).isNull();
        assertThat(readDataRows(files)).isEmpty();
        assertThat(readKeyRows(files)).containsExactly(1L);
    }

    @Test
    void replaceIsAnUpsert() throws Exception {
        DuckLakeTableBuffer buffer = newBuffer();
        buffer.append(DataChangeEvent.replaceEvent(TABLE_ID, record(1L, "a")));
        buffer.append(DataChangeEvent.replaceEvent(TABLE_ID, record(1L, "b")));

        DuckLakeWriteResult files = flushToFiles(buffer, "replace");

        assertThat(readDataRows(files)).containsExactly("1,b");
        assertThat(readKeyRows(files)).containsExactly(1L);
    }

    @Test
    void writesSeparateDataAndKeyParquetFiles() throws Exception {
        DuckLakeTableBuffer buffer = newBuffer();
        buffer.append(insert(1L, "a"));

        DuckLakeWriteResult files = flushToFiles(buffer, "separate");

        assertThat(files.getDataFilePath()).isNotEqualTo(files.getKeyFilePath());
        assertThat(readColumns(files.getDataFilePath())).containsExactly("id", "name");
        assertThat(readColumns(files.getKeyFilePath())).containsExactly("id");
        assertThat(files.getDataRowCount()).isEqualTo(1);
        assertThat(files.getKeyRowCount()).isEqualTo(1);
    }

    @Test
    void buffersEventsWithoutExecutingPerRecordStatements() throws Exception {
        AtomicInteger addBatchCalls = new AtomicInteger();
        AtomicInteger executeUpdateCalls = new AtomicInteger();
        PreparedStatement preparedStatement =
                (PreparedStatement)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("addBatch")) {
                                        addBatchCalls.incrementAndGet();
                                    } else if (method.getName().equals("executeUpdate")) {
                                        executeUpdateCalls.incrementAndGet();
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> defaultValue(method.getReturnType()));
        Connection connection =
                (Connection)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {Connection.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("prepareStatement")) {
                                        return preparedStatement;
                                    }
                                    if (method.getName().equals("createStatement")) {
                                        return statement;
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        DuckLakeTableBuffer buffer =
                new DuckLakeTableBuffer(connection, TABLE_ID, SCHEMA, ZoneId.of("UTC"));

        buffer.append(insert(1L, "a"));

        assertThat(addBatchCalls).hasValue(1);
        assertThat(executeUpdateCalls).hasValue(0);
        buffer.close();
    }

    @Test
    void evaluatesDeduplicationQueriesOnlyOnceDuringFlush() throws Exception {
        List<String> executedSql = new ArrayList<>();
        PreparedStatement preparedStatement =
                (PreparedStatement)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                (proxy, method, args) -> defaultValue(method.getReturnType()));
        Connection connection = recordingConnection(preparedStatement, executedSql);
        DuckLakeTableBuffer buffer =
                new DuckLakeTableBuffer(connection, TABLE_ID, SCHEMA, ZoneId.of("UTC"));
        buffer.append(insert(1L, "a"));

        DuckLakeWriteResult writeResult =
                buffer.flushToFiles(
                        42, 3, 7, 2, 11, "s3://bucket/data.parquet", "s3://bucket/keys.parquet");

        assertThat(executedSql).filteredOn(sql -> sql.contains("row_number() OVER")).hasSize(1);
        assertThat(executedSql).filteredOn(sql -> sql.contains("SELECT DISTINCT")).hasSize(1);
        assertThat(executedSql)
                .filteredOn(sql -> sql.startsWith("CREATE OR REPLACE TEMP TABLE"))
                .singleElement()
                .asString()
                .endsWith("ORDER BY \"id\"");
        assertThat(executedSql)
                .anySatisfy(
                        sql -> assertThat(sql).contains("keys.parquet").contains("RETURN_STATS"));
        assertThat(writeResult.getDataRowCount()).isEqualTo(1);
        assertThat(writeResult.getKeyRowCount()).isEqualTo(1);
    }

    @Test
    void preservesFlushFailureWhenCleanupAlsoFails() throws Exception {
        PreparedStatement preparedStatement =
                (PreparedStatement)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                (proxy, method, args) -> defaultValue(method.getReturnType()));
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("execute")) {
                                        String sql = (String) args[0];
                                        if (sql.startsWith("CREATE OR REPLACE TEMP TABLE")) {
                                            throw new SQLException("materialization failed");
                                        }
                                        if (sql.startsWith("DROP TABLE IF EXISTS")) {
                                            throw new SQLException("cleanup failed");
                                        }
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        Connection connection =
                (Connection)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {Connection.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("prepareStatement")) {
                                        return preparedStatement;
                                    }
                                    if (method.getName().equals("createStatement")) {
                                        return statement;
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        DuckLakeTableBuffer buffer =
                new DuckLakeTableBuffer(connection, TABLE_ID, SCHEMA, ZoneId.of("UTC"));
        buffer.append(insert(1L, "a"));

        assertThatThrownBy(
                        () ->
                                buffer.flushToFiles(
                                        42,
                                        3,
                                        7,
                                        2,
                                        11,
                                        "s3://bucket/data.parquet",
                                        "s3://bucket/keys.parquet"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("materialization failed")
                .satisfies(
                        failure ->
                                assertThat(failure.getSuppressed())
                                        .singleElement()
                                        .extracting(Throwable::getMessage)
                                        .asString()
                                        .contains("cleanup failed"));
    }

    private Connection recordingConnection(
            PreparedStatement preparedStatement, List<String> executedSql) {
        return (Connection)
                Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("prepareStatement")) {
                                return preparedStatement;
                            }
                            if (method.getName().equals("createStatement")) {
                                return recordingStatement(executedSql);
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private Statement recordingStatement(List<String> executedSql) {
        return (Statement)
                Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {Statement.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("execute")
                                    || method.getName().equals("executeQuery")) {
                                executedSql.add((String) args[0]);
                            }
                            if (method.getName().equals("executeQuery")) {
                                AtomicInteger cursor = new AtomicInteger();
                                return Proxy.newProxyInstance(
                                        getClass().getClassLoader(),
                                        new Class<?>[] {ResultSet.class},
                                        (resultProxy, resultMethod, resultArgs) -> {
                                            if (resultMethod.getName().equals("next")) {
                                                return cursor.getAndIncrement() == 0;
                                            }
                                            if (resultMethod.getName().equals("getLong")) {
                                                return 1L;
                                            }
                                            return defaultValue(resultMethod.getReturnType());
                                        });
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private DuckLakeTableBuffer newBuffer() throws Exception {
        return new DuckLakeTableBuffer(
                DriverManager.getConnection("jdbc:duckdb:"), TABLE_ID, SCHEMA, ZoneId.of("UTC"));
    }

    private DuckLakeWriteResult flushToFiles(DuckLakeTableBuffer buffer, String prefix)
            throws Exception {
        String dataPath = tempDir.resolve(prefix + "-data.parquet").toString();
        String keyPath = tempDir.resolve(prefix + "-keys.parquet").toString();
        return buffer.flushToFiles(42, 3, 7, 2, 11, dataPath, keyPath);
    }

    private static DataChangeEvent insert(long id, String name) {
        return DataChangeEvent.insertEvent(TABLE_ID, record(id, name));
    }

    private static DataChangeEvent update(
            long beforeId, String beforeName, long afterId, String afterName) {
        return DataChangeEvent.updateEvent(
                TABLE_ID, record(beforeId, beforeName), record(afterId, afterName));
    }

    private static RecordData record(long id, String name) {
        return GENERATOR.generate(new Object[] {id, BinaryStringData.fromString(name)});
    }

    private static List<String> readDataRows(DuckLakeWriteResult writeResult) throws Exception {
        if (writeResult.getDataFilePath() == null) {
            return new ArrayList<>();
        }
        List<String> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT id, name FROM read_parquet("
                                        + DuckDbSqlUtils.literal(writeResult.getDataFilePath())
                                        + ") ORDER BY id")) {
            while (resultSet.next()) {
                rows.add(resultSet.getLong(1) + "," + resultSet.getString(2));
            }
        }
        return rows;
    }

    private static List<Long> readKeyRows(DuckLakeWriteResult writeResult) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT id FROM read_parquet("
                                        + DuckDbSqlUtils.literal(writeResult.getKeyFilePath())
                                        + ") ORDER BY id")) {
            while (resultSet.next()) {
                rows.add(resultSet.getLong(1));
            }
        }
        return rows;
    }

    private static List<String> readColumns(String path) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT * FROM read_parquet("
                                        + DuckDbSqlUtils.literal(path)
                                        + ") LIMIT 0")) {
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                columns.add(resultSet.getMetaData().getColumnName(i));
            }
            return columns;
        }
    }
}
