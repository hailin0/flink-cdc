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
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckDbConnectionFactory;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogClient;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogOperations;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeTableCoordinator;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeSqlExceptionUtils;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** JDBC implementation of the atomic DuckLake data-file and checkpoint-marker transaction. */
@Internal
public final class DuckLakeJdbcCommitExecutor implements DuckLakeCommitExecutor {

    private static final String CATALOG = DuckLakeCatalogObjects.CATALOG_ALIAS;
    private static final String INTERNAL_SCHEMA = DuckLakeCatalogObjects.INTERNAL_SCHEMA;
    private static final String COMMIT_TABLE = DuckLakeCatalogObjects.COMMIT_TABLE;
    @Nullable private final DuckDbConnectionFactory connectionFactory;
    private DuckLakeCatalogOperations client;
    private DuckLakeTableCoordinator tableCoordinator;
    private DuckLakeDataFileImporter dataFileImporter;
    private boolean initialized;

    public DuckLakeJdbcCommitExecutor(DuckDbConnectionFactory connectionFactory) {
        this.connectionFactory =
                Objects.requireNonNull(connectionFactory, "connectionFactory must not be null");
    }

    @VisibleForTesting
    DuckLakeJdbcCommitExecutor(DuckLakeCatalogOperations client, boolean initialized) {
        this.connectionFactory = null;
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.initialized = initialized;
    }

    @Override
    public Optional<String> findCommittedPlanHash(
            String sinkId, String operatorId, long checkpointId) throws SQLException {
        initialize();
        String sql =
                "SELECT plan_hash FROM "
                        + internalTable()
                        + " WHERE sink_instance_id = "
                        + DuckDbSqlUtils.literal(sinkId)
                        + " AND operator_id = "
                        + DuckDbSqlUtils.literal(operatorId)
                        + " AND checkpoint_id = "
                        + checkpointId;
        List<String> hashes = client.query(sql, resultSet -> resultSet.getString(1));
        if (hashes.size() > 1) {
            throw new SQLException("Multiple DuckLake commit markers found for checkpoint");
        }
        return hashes.stream().findFirst();
    }

    @Override
    public void beginCheckpoint(String sinkId, String operatorId, List<TableId> tables)
            throws SQLException {
        initialize();
        tableCoordinator().prepareCheckpoint(sinkId, operatorId, tables);
        dataFileImporter().clearSchemaCache();
        client.begin();
        try {
            tableCoordinator().lockCheckpoint(sinkId, operatorId, tables);
        } catch (SQLException | RuntimeException failure) {
            try {
                client.rollback();
            } catch (SQLException rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
            }
            throw failure;
        }
    }

    @Override
    public void applyTableChanges(List<DuckLakeCommittable> committables) throws SQLException {
        dataFileImporter().importFiles(committables);
    }

    public void applyTableChanges(DuckLakeCommittable committable) throws SQLException {
        applyTableChanges(Collections.singletonList(committable));
    }

    @Override
    public void recordCheckpoint(
            String sinkId, String operatorId, long checkpointId, String planHash)
            throws SQLException {
        String extraInfo =
                DuckLakeCommitMetadata.extraInfo(sinkId, operatorId, checkpointId, planHash);
        client.execute(
                "CALL "
                        + DuckDbSqlUtils.quoteIdentifier(CATALOG)
                        + ".set_commit_message("
                        + DuckDbSqlUtils.literal(DuckLakeCommitMetadata.AUTHOR)
                        + ", "
                        + DuckDbSqlUtils.literal(
                                DuckLakeCommitMetadata.MESSAGE_PREFIX + checkpointId)
                        + ", extra_info => "
                        + DuckDbSqlUtils.literal(extraInfo)
                        + ")");
        client.execute(
                "INSERT INTO "
                        + internalTable()
                        + " (sink_instance_id, operator_id, checkpoint_id, plan_hash) VALUES ("
                        + DuckDbSqlUtils.literal(sinkId)
                        + ", "
                        + DuckDbSqlUtils.literal(operatorId)
                        + ", "
                        + checkpointId
                        + ", "
                        + DuckDbSqlUtils.literal(planHash)
                        + ")");
    }

    @Override
    public void commit() throws SQLException {
        client.commit();
    }

    @Override
    public void rollback() throws SQLException {
        client.rollback();
    }

    @Override
    public boolean isRetryable(SQLException exception) {
        return DuckLakeSqlExceptionUtils.isRetryable(exception);
    }

    @Override
    public void resetConnection() throws SQLException {
        close();
    }

    @Override
    public void cleanupOrphanFiles(Duration retention) throws SQLException {
        Objects.requireNonNull(retention, "retention must not be null");
        if (retention.compareTo(Duration.ofSeconds(1)) < 0) {
            throw new IllegalArgumentException("Orphan file retention must be positive");
        }
        initialize();
        client.execute(
                "CALL ducklake_delete_orphaned_files("
                        + DuckDbSqlUtils.literal(CATALOG)
                        + ", older_than => current_timestamp - INTERVAL "
                        + DuckDbSqlUtils.literal(retention.getSeconds() + " seconds")
                        + ")");
    }

    @Override
    public void close() throws SQLException {
        if (client != null) {
            client.close();
            client = null;
        }
        dataFileImporter = null;
        tableCoordinator = null;
        initialized = false;
    }

    private void initialize() throws SQLException {
        if (initialized) {
            return;
        }
        if (client == null) {
            client =
                    new DuckLakeCatalogClient(
                            Objects.requireNonNull(
                                    connectionFactory, "connectionFactory must not be null"));
        }
        tableCoordinator();
        dataFileImporter();
        boolean begun = false;
        try {
            client.begin();
            begun = true;
            tableCoordinator().initializeInTransaction();
            client.execute(
                    "CREATE TABLE IF NOT EXISTS "
                            + internalTable()
                            + " (sink_instance_id VARCHAR NOT NULL, operator_id VARCHAR NOT NULL, "
                            + "checkpoint_id BIGINT NOT NULL, plan_hash VARCHAR NOT NULL, "
                            + "committed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp)");
            client.commit();
            begun = false;
            initialized = true;
        } catch (SQLException | RuntimeException exception) {
            if (begun) {
                try {
                    client.rollback();
                } catch (SQLException rollbackFailure) {
                    exception.addSuppressed(rollbackFailure);
                }
            }
            throw exception;
        }
    }

    private static String internalTable() {
        return DuckDbSqlUtils.qualifiedName(CATALOG, INTERNAL_SCHEMA, COMMIT_TABLE);
    }

    private DuckLakeDataFileImporter dataFileImporter() {
        if (dataFileImporter == null) {
            dataFileImporter = new DuckLakeDataFileImporter(client);
        }
        return dataFileImporter;
    }

    private DuckLakeTableCoordinator tableCoordinator() {
        if (tableCoordinator == null) {
            tableCoordinator = new DuckLakeTableCoordinator(client);
        }
        return tableCoordinator;
    }
}
