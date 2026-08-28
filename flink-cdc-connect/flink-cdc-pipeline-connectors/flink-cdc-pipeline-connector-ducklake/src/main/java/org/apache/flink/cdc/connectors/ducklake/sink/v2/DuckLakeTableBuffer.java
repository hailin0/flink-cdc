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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeTypeUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Append-only per-table buffer that materializes the last image of each primary key on flush. */
@Internal
public final class DuckLakeTableBuffer implements AutoCloseable {

    private static final String EVENT_TABLE_PREFIX = "flink_cdc_event_buffer_";
    private static final int JDBC_BATCH_SIZE = 1024;

    private final Connection connection;
    private final boolean closeConnectionOnClose;
    private final String eventTable;
    private final String latestRowsTable;
    private final String sequenceColumn;
    private final String deletedColumn;
    private final String rankColumn;
    private final TableId tableId;
    private final Schema schema;
    private final List<DuckLakeColumnMetadata> columnMetadata;
    private final List<Integer> primaryKeyPositions;
    private final List<DuckLakeTypeUtils.FieldBinder> columnBinders;
    private final PreparedStatement insertEventStatement;

    private long bufferedEventCount;
    private int pendingBatchCount;
    private boolean closed;

    public DuckLakeTableBuffer(
            Connection connection, TableId tableId, Schema schema, ZoneId pipelineZone)
            throws SQLException {
        this(connection, tableId, schema, pipelineZone, "standalone", true);
    }

    DuckLakeTableBuffer(
            Connection connection,
            TableId tableId,
            Schema schema,
            ZoneId pipelineZone,
            String bufferId,
            boolean closeConnectionOnClose)
            throws SQLException {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.closeConnectionOnClose = closeConnectionOnClose;
        this.tableId = Objects.requireNonNull(tableId, "tableId must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(pipelineZone, "pipelineZone must not be null");
        if (bufferId == null || !bufferId.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("bufferId contains invalid characters");
        }
        this.eventTable = EVENT_TABLE_PREFIX + bufferId;
        this.latestRowsTable = eventTable + "_latest";
        validateSchema(schema);
        this.sequenceColumn = uniqueMetadataColumn("__flink_cdc_sequence_" + bufferId);
        this.deletedColumn = uniqueMetadataColumn("__flink_cdc_deleted_" + bufferId);
        this.rankColumn = uniqueMetadataColumn("__flink_cdc_rank_" + bufferId);
        this.columnMetadata = createColumnMetadata(schema);
        this.primaryKeyPositions = primaryKeyPositions(schema);
        this.columnBinders = createBinders(schema, allColumnPositions(schema), pipelineZone);

        try {
            createTemporaryTable();
            this.insertEventStatement = connection.prepareStatement(insertEventSql());
        } catch (SQLException | RuntimeException e) {
            closeConnectionAfterInitializationFailure(e);
            throw e;
        }
    }

    public void append(DataChangeEvent event) throws SQLException {
        ensureOpen();
        if (!tableId.equals(event.tableId())) {
            throw new IllegalArgumentException(
                    "Event table " + event.tableId() + " does not match buffer table " + tableId);
        }
        if (event.op() == OperationType.UPDATE) {
            validatePrimaryKeyUnchanged(event.before(), event.after());
        }
        RecordData record = event.op() == OperationType.DELETE ? event.before() : event.after();
        bind(
                columnBinders,
                insertEventStatement,
                Objects.requireNonNull(record, "CDC row image must not be null"));
        insertEventStatement.setLong(schema.getColumnCount() + 1, bufferedEventCount);
        insertEventStatement.setBoolean(
                schema.getColumnCount() + 2, event.op() == OperationType.DELETE);
        insertEventStatement.addBatch();
        pendingBatchCount++;
        if (pendingBatchCount >= JDBC_BATCH_SIZE) {
            flushPendingBatch();
        }
        bufferedEventCount++;
    }

    public long getBufferedEventCount() {
        return bufferedEventCount;
    }

    public DuckLakeWriteResult flushToFiles(
            long checkpointId,
            int schemaBatchIndex,
            int subtaskId,
            int attemptNumber,
            long fileSequence,
            String dataFilePath,
            String keyFilePath)
            throws SQLException {
        ensureOpen();
        validateOutputPath(dataFilePath, "dataFilePath");
        validateOutputPath(keyFilePath, "keyFilePath");
        if (bufferedEventCount == 0) {
            throw new IllegalStateException("Cannot flush an empty DuckLake table buffer");
        }

        DuckLakeWriteResult writeResult;
        try {
            flushPendingBatch();
            String latestRows = latestRowsQuery();
            String distinctKeys = distinctKeysQuery();
            materializeLatestRows(latestRows);
            String materializedLatestRows =
                    "SELECT * FROM " + DuckDbSqlUtils.quoteIdentifier(latestRowsTable);
            long dataRowCount = queryRowCount(materializedLatestRows);
            String actualDataPath = null;
            if (dataRowCount > 0) {
                copyToParquet(materializedLatestRows, dataFilePath);
                actualDataPath = dataFilePath;
            }
            long keyRowCount = copyToParquetWithStats(distinctKeys, keyFilePath);
            if (keyRowCount == 0) {
                throw new IllegalStateException(
                        "A non-empty table buffer must contain delete keys");
            }
            writeResult =
                    new DuckLakeWriteResult(
                            tableId,
                            checkpointId,
                            schemaBatchIndex,
                            subtaskId,
                            attemptNumber,
                            fileSequence,
                            columnMetadata,
                            schema.primaryKeys(),
                            actualDataPath,
                            dataRowCount,
                            keyFilePath,
                            keyRowCount);
        } catch (SQLException | RuntimeException failure) {
            try {
                close();
            } catch (SQLException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        close();
        return writeResult;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        SQLException failure = null;
        failure = closeStatement(insertEventStatement, failure);
        for (String table : new String[] {latestRowsTable, eventTable}) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + DuckDbSqlUtils.quoteIdentifier(table));
            } catch (SQLException e) {
                failure = addFailure(failure, e);
            }
        }
        if (closeConnectionOnClose) {
            try {
                connection.close();
            } catch (SQLException e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void createTemporaryTable() throws SQLException {
        List<String> definitions = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            String definition =
                    DuckDbSqlUtils.quoteIdentifier(column.getName())
                            + " "
                            + DuckLakeTypeUtils.toDuckDbType(column.getType());
            if (!column.getType().isNullable()) {
                definition += " NOT NULL";
            }
            definitions.add(definition);
        }
        definitions.add(DuckDbSqlUtils.quoteIdentifier(sequenceColumn) + " BIGINT NOT NULL");
        definitions.add(DuckDbSqlUtils.quoteIdentifier(deletedColumn) + " BOOLEAN NOT NULL");

        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TEMP TABLE "
                            + DuckDbSqlUtils.quoteIdentifier(eventTable)
                            + " ("
                            + String.join(", ", definitions)
                            + ")");
        }
    }

    private String insertEventSql() {
        return "INSERT INTO "
                + DuckDbSqlUtils.quoteIdentifier(eventTable)
                + " ("
                + quotedNames(schema.getColumnNames())
                + ", "
                + DuckDbSqlUtils.quoteIdentifier(sequenceColumn)
                + ", "
                + DuckDbSqlUtils.quoteIdentifier(deletedColumn)
                + ") VALUES ("
                + placeholders(schema.getColumnCount() + 2)
                + ")";
    }

    private String latestRowsQuery() {
        return "SELECT "
                + quotedNames(schema.getColumnNames())
                + " FROM (SELECT *, row_number() OVER (PARTITION BY "
                + quotedNames(schema.primaryKeys())
                + " ORDER BY "
                + DuckDbSqlUtils.quoteIdentifier(sequenceColumn)
                + " DESC) AS "
                + DuckDbSqlUtils.quoteIdentifier(rankColumn)
                + " FROM "
                + DuckDbSqlUtils.quoteIdentifier(eventTable)
                + ") WHERE "
                + DuckDbSqlUtils.quoteIdentifier(rankColumn)
                + " = 1 AND NOT "
                + DuckDbSqlUtils.quoteIdentifier(deletedColumn)
                + " ORDER BY "
                + quotedNames(schema.primaryKeys());
    }

    private String distinctKeysQuery() {
        return "SELECT DISTINCT "
                + quotedNames(schema.primaryKeys())
                + " FROM "
                + DuckDbSqlUtils.quoteIdentifier(eventTable);
    }

    private long queryRowCount(String query) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT COUNT(*) FROM (" + query + ")")) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private void materializeLatestRows(String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE TEMP TABLE "
                            + DuckDbSqlUtils.quoteIdentifier(latestRowsTable)
                            + " AS "
                            + query);
        }
    }

    private void copyToParquet(String query, String path) throws SQLException {
        String sql =
                "COPY (" + query + ") TO " + DuckDbSqlUtils.literal(path) + " (FORMAT PARQUET)";
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private long copyToParquetWithStats(String query, String path) throws SQLException {
        String sql =
                "COPY ("
                        + query
                        + ") TO "
                        + DuckDbSqlUtils.literal(path)
                        + " (FORMAT PARQUET, RETURN_STATS)";
        long rowCount = 0;
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                rowCount += resultSet.getLong("count");
            }
        }
        return rowCount;
    }

    private void flushPendingBatch() throws SQLException {
        if (pendingBatchCount == 0) {
            return;
        }
        insertEventStatement.executeBatch();
        pendingBatchCount = 0;
    }

    private String uniqueMetadataColumn(String candidate) {
        String result = candidate;
        while (schema.getColumnNames().contains(result)) {
            result += "_";
        }
        return result;
    }

    private void validatePrimaryKeyUnchanged(RecordData before, RecordData after) {
        Objects.requireNonNull(before, "before record must not be null");
        Objects.requireNonNull(after, "after record must not be null");
        for (int i = 0; i < primaryKeyPositions.size(); i++) {
            int position = primaryKeyPositions.get(i);
            RecordData.FieldGetter getter =
                    RecordData.createFieldGetter(
                            schema.getColumns().get(position).getType(), position);
            Object beforeValue = getter.getFieldOrNull(before);
            Object afterValue = getter.getFieldOrNull(after);
            if (!Objects.deepEquals(beforeValue, afterValue)) {
                throw new IllegalArgumentException(
                        "Primary-key-changing UPDATE is not supported for table " + tableId);
            }
        }
    }

    private static void bind(
            List<DuckLakeTypeUtils.FieldBinder> binders,
            PreparedStatement statement,
            RecordData record)
            throws SQLException {
        for (int i = 0; i < binders.size(); i++) {
            binders.get(i).bind(statement, i + 1, record);
        }
    }

    private static List<DuckLakeTypeUtils.FieldBinder> createBinders(
            Schema schema, List<Integer> positions, ZoneId pipelineZone) {
        List<DuckLakeTypeUtils.FieldBinder> binders = new ArrayList<>();
        for (int position : positions) {
            binders.add(
                    DuckLakeTypeUtils.createFieldBinder(
                            schema.getColumns().get(position).getType(), position, pipelineZone));
        }
        return binders;
    }

    private static List<Integer> allColumnPositions(Schema schema) {
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            positions.add(i);
        }
        return positions;
    }

    private static List<Integer> primaryKeyPositions(Schema schema) {
        List<Integer> positions = new ArrayList<>();
        for (String primaryKey : schema.primaryKeys()) {
            int position = schema.getColumnNames().indexOf(primaryKey);
            if (position < 0) {
                throw new IllegalArgumentException(
                        "Primary key column does not exist: " + primaryKey);
            }
            positions.add(position);
        }
        return positions;
    }

    private static List<DuckLakeColumnMetadata> createColumnMetadata(Schema schema) {
        return schema.getColumns().stream()
                .map(column -> new DuckLakeColumnMetadata(column.getName(), column.getType()))
                .collect(Collectors.toList());
    }

    static void validateSchema(Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            throw new IllegalArgumentException("DuckLake CDC sink requires a primary key");
        }
        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()) {
                throw new IllegalArgumentException(
                        "DuckLake CDC sink supports only physical columns: " + column.getName());
            }
            DuckLakeTypeUtils.toDuckDbType(column.getType());
        }
        for (String primaryKey : schema.primaryKeys()) {
            Column column =
                    schema.getColumn(primaryKey)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Primary key column does not exist: "
                                                            + primaryKey));
            if (column.getType().isNullable()) {
                throw new IllegalArgumentException(
                        "Primary key column must be NOT NULL: " + primaryKey);
            }
        }
    }

    private static String quotedNames(List<String> names) {
        return names.stream()
                .map(DuckDbSqlUtils::quoteIdentifier)
                .collect(Collectors.joining(", "));
    }

    private static String placeholders(int count) {
        List<String> placeholders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            placeholders.add("?");
        }
        return String.join(", ", placeholders);
    }

    private static void validateOutputPath(String path, String name) {
        Objects.requireNonNull(path, name + " must not be null");
        if (path.isEmpty()
                || path.indexOf('\0') >= 0
                || path.indexOf('\n') >= 0
                || path.indexOf('\r') >= 0) {
            throw new IllegalArgumentException(name + " contains invalid characters");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("DuckLake table buffer is closed");
        }
    }

    private static SQLException closeStatement(
            PreparedStatement statement, SQLException previousFailure) {
        try {
            statement.close();
            return previousFailure;
        } catch (SQLException e) {
            return addFailure(previousFailure, e);
        }
    }

    private static SQLException addFailure(SQLException previousFailure, SQLException failure) {
        if (previousFailure == null) {
            return failure;
        }
        previousFailure.addSuppressed(failure);
        return previousFailure;
    }

    private void closeConnectionAfterInitializationFailure(Exception failure) {
        if (closeConnectionOnClose) {
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }
}
