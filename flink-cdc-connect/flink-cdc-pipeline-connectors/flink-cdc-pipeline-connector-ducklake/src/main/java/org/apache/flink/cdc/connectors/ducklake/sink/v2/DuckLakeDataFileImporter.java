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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogOperations;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeTypeUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Validates and imports staged Parquet files into one DuckLake transaction. */
final class DuckLakeDataFileImporter {

    private static final String TEMPORARY_KEY_TABLE_PREFIX = "keys_";

    private final DuckLakeCatalogOperations client;
    private final Map<TableId, List<SchemaColumn>> targetSchemas = new HashMap<>();

    DuckLakeDataFileImporter(DuckLakeCatalogOperations client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    void clearSchemaCache() {
        targetSchemas.clear();
    }

    void importFiles(List<DuckLakeCommittable> committables) throws SQLException {
        if (committables.isEmpty()) {
            throw new IllegalArgumentException("DuckLake commit batch must not be empty");
        }
        DuckLakeCommittable first = committables.get(0);
        DuckLakeWriteResult firstResult = first.getWriteResult();
        validateBatch(committables, firstResult);
        TableId tableId = firstResult.getTableId();
        String schema =
                tableId.getSchemaName() == null
                        ? DuckLakeCatalogObjects.DEFAULT_SCHEMA
                        : tableId.getSchemaName();
        String keyTable = TEMPORARY_KEY_TABLE_PREFIX + first.getPayloadHash().substring(0, 16);
        List<SchemaFlags> schemaFlags = new ArrayList<>(committables.size());
        for (DuckLakeCommittable committable : committables) {
            schemaFlags.add(schemaFlags(committable.getWriteResult(), schema));
        }
        client.execute(
                "CREATE OR REPLACE TEMP TABLE "
                        + DuckDbSqlUtils.quoteIdentifier(keyTable)
                        + " AS SELECT * FROM read_parquet("
                        + keyFilePathList(committables)
                        + ")");
        try {
            validateKeyTableSchema(firstResult, keyTable);
            if (committables.size() > 1) {
                validateNoDuplicateKeys(keyTable, firstResult);
            }
            client.execute(
                    "DELETE FROM "
                            + DuckDbSqlUtils.qualifiedName(
                                    DuckLakeCatalogObjects.CATALOG_ALIAS,
                                    schema,
                                    tableId.getTableName())
                            + " AS t USING "
                            + DuckDbSqlUtils.quoteIdentifier(keyTable)
                            + " AS k WHERE "
                            + DuckDbSqlUtils.primaryKeyPredicate(
                                    "t", "k", firstResult.getPrimaryKeys()));
            for (int i = 0; i < committables.size(); i++) {
                DuckLakeWriteResult current = committables.get(i).getWriteResult();
                if (current.getDataFilePath() == null) {
                    continue;
                }
                SchemaFlags currentFlags = schemaFlags.get(i);
                client.execute(
                        "CALL ducklake_add_data_files("
                                + DuckDbSqlUtils.literal(DuckLakeCatalogObjects.CATALOG_ALIAS)
                                + ", "
                                + DuckDbSqlUtils.literal(tableId.getTableName())
                                + ", "
                                + DuckDbSqlUtils.literal(current.getDataFilePath())
                                + ", schema => "
                                + DuckDbSqlUtils.literal(schema)
                                + ", allow_missing => "
                                + currentFlags.allowMissing
                                + ", ignore_extra_columns => "
                                + currentFlags.ignoreExtra
                                + ")");
            }
        } catch (SQLException | RuntimeException failure) {
            try {
                dropTemporaryTable(keyTable);
            } catch (SQLException | RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        dropTemporaryTable(keyTable);
    }

    private void dropTemporaryTable(String table) throws SQLException {
        client.execute("DROP TABLE IF EXISTS " + DuckDbSqlUtils.quoteIdentifier(table));
    }

    private static void validateBatch(
            List<DuckLakeCommittable> committables, DuckLakeWriteResult first) {
        for (DuckLakeCommittable committable : committables) {
            DuckLakeWriteResult current = committable.getWriteResult();
            if (!first.getTableId().equals(current.getTableId())
                    || first.getSchemaBatchIndex() != current.getSchemaBatchIndex()
                    || !first.getColumns().equals(current.getColumns())
                    || !first.getPrimaryKeys().equals(current.getPrimaryKeys())) {
                throw new IllegalArgumentException(
                        "DuckLake commit batch contains incompatible write results");
            }
        }
    }

    private void validateNoDuplicateKeys(String keyTable, DuckLakeWriteResult writeResult)
            throws SQLException {
        String columns =
                writeResult.getPrimaryKeys().stream()
                        .map(DuckDbSqlUtils::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        List<Integer> duplicates =
                client.query(
                        "SELECT 1 FROM "
                                + DuckDbSqlUtils.quoteIdentifier(keyTable)
                                + " GROUP BY "
                                + columns
                                + " HAVING COUNT(*) > 1 LIMIT 1",
                        resultSet -> resultSet.getInt(1));
        if (!duplicates.isEmpty()) {
            throw new SQLException(
                    "The same primary key was produced by multiple DuckLake writer files");
        }
    }

    private static String keyFilePathList(List<DuckLakeCommittable> committables) {
        return committables.stream()
                .map(DuckLakeCommittable::getWriteResult)
                .map(DuckLakeWriteResult::getKeyFilePath)
                .map(DuckDbSqlUtils::literal)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private SchemaFlags schemaFlags(DuckLakeWriteResult writeResult, String schema)
            throws SQLException {
        String table =
                DuckDbSqlUtils.qualifiedName(
                        DuckLakeCatalogObjects.CATALOG_ALIAS,
                        schema,
                        writeResult.getTableId().getTableName());
        List<SchemaColumn> target = targetSchemas.get(writeResult.getTableId());
        if (target == null) {
            target =
                    client.query(
                            "DESCRIBE SELECT * FROM " + table,
                            resultSet ->
                                    new SchemaColumn(
                                            resultSet.getString(1),
                                            resultSet.getString(2),
                                            resultSet.getString(3)));
            targetSchemas.put(writeResult.getTableId(), target);
        }
        List<SchemaColumn> file = new ArrayList<>();
        if (writeResult.getDataFilePath() == null) {
            for (DuckLakeColumnMetadata column : writeResult.getColumns()) {
                file.add(
                        new SchemaColumn(
                                column.getName(),
                                DuckLakeTypeUtils.toDuckDbType(column.getDataType()),
                                column.getDataType().isNullable() ? "YES" : "NO"));
            }
        } else {
            file =
                    client.query(
                            "DESCRIBE SELECT * FROM read_parquet("
                                    + DuckDbSqlUtils.literal(writeResult.getDataFilePath())
                                    + ")",
                            resultSet ->
                                    new SchemaColumn(
                                            resultSet.getString(1),
                                            resultSet.getString(2),
                                            resultSet.getString(3)));
            if (file.size() != writeResult.getColumns().size()) {
                throw new SQLException("Parquet schema does not match the write result");
            }
            for (int i = 0; i < file.size(); i++) {
                SchemaColumn actual = file.get(i);
                DuckLakeColumnMetadata declared = writeResult.getColumns().get(i);
                if (!actual.name.equals(declared.getName())
                        || !DuckLakeTypeUtils.isCompatibleParquetType(
                                declared.getDataType(), actual.type)) {
                    throw new SQLException(
                            "Parquet column name or type does not match the write result");
                }
            }
        }
        List<String> targetNames =
                target.stream().map(column -> column.name).collect(Collectors.toList());
        List<String> fileNames =
                file.stream().map(column -> column.name).collect(Collectors.toList());
        Map<String, DuckLakeColumnMetadata> declaredByName =
                writeResult.getColumns().stream()
                        .collect(
                                Collectors.toMap(
                                        DuckLakeColumnMetadata::getName, column -> column));
        for (SchemaColumn targetColumn : target) {
            DuckLakeColumnMetadata declared = declaredByName.get(targetColumn.name);
            if (declared == null && !targetColumn.nullable) {
                throw new SQLException(
                        "Parquet file is missing non-nullable target column " + targetColumn.name);
            }
            if (declared != null
                    && !DuckLakeTypeUtils.isCompatibleTargetType(
                            declared.getDataType(), targetColumn.type)) {
                throw new SQLException(
                        "DuckLake target column type is incompatible with the write result");
            }
        }
        return new SchemaFlags(
                !fileNames.containsAll(targetNames), !targetNames.containsAll(fileNames));
    }

    private void validateKeyTableSchema(DuckLakeWriteResult writeResult, String keyTable)
            throws SQLException {
        List<SchemaColumn> keyFile =
                client.query(
                        "DESCRIBE SELECT * FROM " + DuckDbSqlUtils.quoteIdentifier(keyTable),
                        resultSet ->
                                new SchemaColumn(
                                        resultSet.getString(1),
                                        resultSet.getString(2),
                                        resultSet.getString(3)));
        Map<String, DuckLakeColumnMetadata> declaredByName =
                writeResult.getColumns().stream()
                        .collect(
                                Collectors.toMap(
                                        DuckLakeColumnMetadata::getName, column -> column));
        if (keyFile.size() != writeResult.getPrimaryKeys().size()) {
            throw new SQLException(
                    "Key Parquet schema does not match the primary-key write result");
        }
        for (int i = 0; i < keyFile.size(); i++) {
            SchemaColumn actual = keyFile.get(i);
            String primaryKey = writeResult.getPrimaryKeys().get(i);
            DuckLakeColumnMetadata declared = declaredByName.get(primaryKey);
            if (!actual.name.equals(primaryKey)
                    || declared == null
                    || !DuckLakeTypeUtils.isCompatibleParquetType(
                            declared.getDataType(), actual.type)) {
                throw new SQLException(
                        "Key Parquet column name or type does not match the primary-key write result");
            }
        }
    }

    private static final class SchemaFlags {
        private final boolean allowMissing;
        private final boolean ignoreExtra;

        private SchemaFlags(boolean allowMissing, boolean ignoreExtra) {
            this.allowMissing = allowMissing;
            this.ignoreExtra = ignoreExtra;
        }
    }

    private static final class SchemaColumn {
        private final String name;
        private final String type;
        private final boolean nullable;

        private SchemaColumn(String name, String type, String nullable) {
            this.name = name;
            this.type = type;
            this.nullable = "YES".equalsIgnoreCase(nullable);
        }
    }
}
