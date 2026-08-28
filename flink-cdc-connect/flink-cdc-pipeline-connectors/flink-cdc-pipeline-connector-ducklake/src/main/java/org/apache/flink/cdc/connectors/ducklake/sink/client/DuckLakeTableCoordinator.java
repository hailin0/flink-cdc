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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Creates and acquires connector-owned table fences used by DDL and data transactions. */
@Internal
public final class DuckLakeTableCoordinator {

    private final DuckLakeCatalogOperations client;
    private final Set<TableId> preparedTables = new HashSet<>();

    public DuckLakeTableCoordinator(DuckLakeCatalogOperations client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    public void prepareTable(TableId tableId) throws SQLException {
        if (preparedTables.contains(tableId)) {
            return;
        }
        boolean transactionStarted = false;
        try {
            client.begin();
            transactionStarted = true;
            client.execute(
                    "CREATE SCHEMA IF NOT EXISTS "
                            + DuckDbSqlUtils.qualifiedName(
                                    DuckLakeCatalogObjects.CATALOG_ALIAS,
                                    DuckLakeCatalogObjects.INTERNAL_SCHEMA));
            client.execute(
                    "CREATE TABLE IF NOT EXISTS "
                            + coordinationTable()
                            + " (sink_instance_id VARCHAR NOT NULL, operator_id VARCHAR NOT NULL, "
                            + "lock_version BIGINT NOT NULL)");
            ensureRow(DuckLakeCatalogObjects.GLOBAL_LOCK_ID, DuckLakeCatalogObjects.GLOBAL_LOCK_ID);
            client.execute(
                    updateSql(
                            DuckLakeCatalogObjects.GLOBAL_LOCK_ID,
                            DuckLakeCatalogObjects.GLOBAL_LOCK_ID));
            ensureRow(
                    DuckLakeCatalogObjects.TABLE_LOCK_ID,
                    DuckLakeCatalogObjects.tableLockKey(tableId));
            client.commit();
            transactionStarted = false;
            preparedTables.add(tableId);
        } catch (SQLException | RuntimeException failure) {
            if (transactionStarted) {
                try {
                    client.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
            }
            throw failure;
        }
    }

    public void lockTable(TableId tableId) throws SQLException {
        client.execute(
                updateSql(
                        DuckLakeCatalogObjects.TABLE_LOCK_ID,
                        DuckLakeCatalogObjects.tableLockKey(tableId)));
    }

    /** Initializes connector-owned coordination objects inside the current transaction. */
    public void initializeInTransaction() throws SQLException {
        client.execute(
                "CREATE SCHEMA IF NOT EXISTS "
                        + DuckDbSqlUtils.qualifiedName(
                                DuckLakeCatalogObjects.CATALOG_ALIAS,
                                DuckLakeCatalogObjects.INTERNAL_SCHEMA));
        List<String> coordinationTables =
                client.query(
                        "SELECT table_name FROM information_schema.tables WHERE "
                                + "table_catalog = "
                                + DuckDbSqlUtils.literal(DuckLakeCatalogObjects.CATALOG_ALIAS)
                                + " AND table_schema = "
                                + DuckDbSqlUtils.literal(DuckLakeCatalogObjects.INTERNAL_SCHEMA)
                                + " AND table_name = "
                                + DuckDbSqlUtils.literal(DuckLakeCatalogObjects.COORDINATION_TABLE),
                        resultSet -> resultSet.getString(1));
        if (coordinationTables.isEmpty()) {
            client.execute(
                    "CREATE TABLE "
                            + coordinationTable()
                            + " (sink_instance_id VARCHAR NOT NULL, operator_id VARCHAR NOT NULL, "
                            + "lock_version BIGINT NOT NULL)");
            insertRow(DuckLakeCatalogObjects.GLOBAL_LOCK_ID, DuckLakeCatalogObjects.GLOBAL_LOCK_ID);
        } else if (coordinationTables.size() == 1) {
            validateGlobalRow();
        } else {
            throw new SQLException("Multiple DuckLake coordination tables found");
        }
    }

    /** Prepares checkpoint and table lock rows before the checkpoint transaction starts. */
    public void prepareCheckpoint(String sinkId, String operatorId, List<TableId> tables)
            throws SQLException {
        ensureRowWithGlobalFence(sinkId, operatorId);
        for (TableId table : orderedTables(tables)) {
            ensureRowWithGlobalFence(
                    DuckLakeCatalogObjects.TABLE_LOCK_ID,
                    DuckLakeCatalogObjects.tableLockKey(table));
        }
    }

    /** Acquires checkpoint and table locks inside the current checkpoint transaction. */
    public void lockCheckpoint(String sinkId, String operatorId, List<TableId> tables)
            throws SQLException {
        client.execute(updateSql(sinkId, operatorId));
        for (TableId table : orderedTables(tables)) {
            client.execute(
                    updateSql(
                            DuckLakeCatalogObjects.TABLE_LOCK_ID,
                            DuckLakeCatalogObjects.tableLockKey(table)));
        }
    }

    private void ensureRow(String sinkId, String operatorId) throws SQLException {
        List<Long> rows = versions(sinkId, operatorId);
        if (rows.size() > 1) {
            throw new SQLException("Multiple DuckLake coordination rows found for resource");
        }
        if (rows.isEmpty()) {
            insertRow(sinkId, operatorId);
        }
    }

    private void ensureRowWithGlobalFence(String sinkId, String operatorId) throws SQLException {
        List<Long> versions = versions(sinkId, operatorId);
        if (versions.size() > 1) {
            throw new SQLException("Multiple DuckLake coordination rows found for resource");
        }
        if (!versions.isEmpty()) {
            return;
        }
        boolean transactionStarted = false;
        try {
            client.begin();
            transactionStarted = true;
            client.execute(
                    updateSql(
                            DuckLakeCatalogObjects.GLOBAL_LOCK_ID,
                            DuckLakeCatalogObjects.GLOBAL_LOCK_ID));
            versions = versions(sinkId, operatorId);
            if (versions.size() > 1) {
                throw new SQLException("Multiple DuckLake coordination rows found for resource");
            }
            if (versions.isEmpty()) {
                insertRow(sinkId, operatorId);
            }
            client.commit();
            transactionStarted = false;
        } catch (SQLException | RuntimeException failure) {
            if (transactionStarted) {
                try {
                    client.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
            }
            throw failure;
        }
    }

    private void validateGlobalRow() throws SQLException {
        if (versions(DuckLakeCatalogObjects.GLOBAL_LOCK_ID, DuckLakeCatalogObjects.GLOBAL_LOCK_ID)
                        .size()
                != 1) {
            throw new SQLException("DuckLake global coordination row is missing or duplicated");
        }
    }

    private List<Long> versions(String sinkId, String operatorId) throws SQLException {
        return client.query(
                "SELECT lock_version FROM "
                        + coordinationTable()
                        + " WHERE "
                        + predicate(sinkId, operatorId),
                resultSet -> resultSet.getLong(1));
    }

    private void insertRow(String sinkId, String operatorId) throws SQLException {
        client.execute(
                "INSERT INTO "
                        + coordinationTable()
                        + " (sink_instance_id, operator_id, lock_version) VALUES ("
                        + DuckDbSqlUtils.literal(sinkId)
                        + ", "
                        + DuckDbSqlUtils.literal(operatorId)
                        + ", 0)");
    }

    private static List<TableId> orderedTables(List<TableId> tables) {
        Objects.requireNonNull(tables, "tables must not be null");
        Map<String, TableId> tablesByLockKey = new TreeMap<>();
        for (TableId table : tables) {
            tablesByLockKey.putIfAbsent(DuckLakeCatalogObjects.tableLockKey(table), table);
        }
        return new ArrayList<>(tablesByLockKey.values());
    }

    private static String updateSql(String sinkId, String operatorId) {
        return "UPDATE "
                + coordinationTable()
                + " SET lock_version = lock_version + 1 WHERE "
                + predicate(sinkId, operatorId);
    }

    private static String predicate(String sinkId, String operatorId) {
        return "sink_instance_id = "
                + DuckDbSqlUtils.literal(sinkId)
                + " AND operator_id = "
                + DuckDbSqlUtils.literal(operatorId);
    }

    private static String coordinationTable() {
        return DuckDbSqlUtils.qualifiedName(
                DuckLakeCatalogObjects.CATALOG_ALIAS,
                DuckLakeCatalogObjects.INTERNAL_SCHEMA,
                DuckLakeCatalogObjects.COORDINATION_TABLE);
    }
}
