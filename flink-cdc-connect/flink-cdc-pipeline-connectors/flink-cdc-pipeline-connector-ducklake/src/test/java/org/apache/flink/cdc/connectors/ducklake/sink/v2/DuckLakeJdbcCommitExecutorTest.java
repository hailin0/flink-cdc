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
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogOperations;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeJdbcCommitExecutorTest {

    @Test
    void rejectsParquetTypeThatDiffersFromWriteResult() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO"), row("amount", "INTEGER", "YES")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "BIGINT", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        assertThatThrownBy(() -> executor.applyTableChanges(committable()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Parquet")
                .hasMessageContaining("type");
    }

    @Test
    void rejectsTargetTypeThatIsIncompatibleWithWriteResult() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO"), row("amount", "VARCHAR", "YES")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "INTEGER", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        assertThatThrownBy(() -> executor.applyTableChanges(committable()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("target")
                .hasMessageContaining("type");
    }

    @Test
    void rejectsMissingNonNullableTargetColumn() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(
                                row("id", "BIGINT", "NO"),
                                row("amount", "INTEGER", "YES"),
                                row("required", "VARCHAR", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "INTEGER", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        assertThatThrownBy(() -> executor.applyTableChanges(committable()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("non-nullable")
                .hasMessageContaining("required");
    }

    @Test
    void registersFilesByNameWhenTargetColumnsAreReordered() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("amount", "INTEGER", "YES"), row("id", "BIGINT", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "INTEGER", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        executor.applyTableChanges(committable());

        assertThat(client.executedSql)
                .anySatisfy(
                        sql ->
                                assertThat(sql)
                                        .startsWith("CALL ducklake_add_data_files")
                                        .contains("allow_missing => false")
                                        .contains("ignore_extra_columns => false"));
    }

    @Test
    void rejectsKeyParquetTypeThatDiffersFromPrimaryKeyWriteResult() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO"), row("amount", "INTEGER", "YES")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "INTEGER", "YES")),
                        Collections.singletonList(row("id", "VARCHAR", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        assertThatThrownBy(() -> executor.applyTableChanges(committable()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Key Parquet")
                .hasMessageContaining("type");
    }

    @Test
    void validatesTargetSchemaForKeyOnlyDeleteFile() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "VARCHAR", "NO"), row("amount", "INTEGER", "YES")),
                        Collections.emptyList());
        DuckLakeJdbcCommitExecutor executor = executor(client);

        assertThatThrownBy(() -> executor.applyTableChanges(keyOnlyCommittable()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("target")
                .hasMessageContaining("type");
    }

    @Test
    void allowsNullableTargetColumnsMissingAtAnyPosition() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(
                                row("id", "BIGINT", "NO"),
                                row("added", "VARCHAR", "YES"),
                                row("amount", "INTEGER", "YES")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "INTEGER", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        executor.applyTableChanges(committable());

        assertThat(client.executedSql)
                .anySatisfy(
                        sql ->
                                assertThat(sql)
                                        .startsWith("CALL ducklake_add_data_files")
                                        .contains("allow_missing => true")
                                        .contains("ignore_extra_columns => false"));
    }

    @Test
    void deletesOnceAndRegistersEveryDataFileInBatch() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO"), row("amount", "INTEGER", "YES")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "INTEGER", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);
        DuckLakeCommittable first =
                committable(
                        "s3://bucket/data-0.parquet",
                        "s3://bucket/keys-0.parquet",
                        0,
                        "0123456789abcdef");
        DuckLakeCommittable second =
                committable(
                        "s3://bucket/data-1.parquet",
                        "s3://bucket/keys-1.parquet",
                        1,
                        "fedcba9876543210");

        executor.applyTableChanges(Arrays.asList(first, second));

        assertThat(client.executedSql).filteredOn(sql -> sql.startsWith("DELETE FROM")).hasSize(1);
        assertThat(client.executedSql)
                .filteredOn(sql -> sql.startsWith("CALL ducklake_add_data_files"))
                .hasSize(2);
        assertThat(client.executedSql)
                .anySatisfy(
                        sql ->
                                assertThat(sql)
                                        .contains(
                                                "['s3://bucket/keys-0.parquet', "
                                                        + "'s3://bucket/keys-1.parquet']"));
    }

    @Test
    void readsRemoteKeyFilesOnlyOncePerTableBatch() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO"), row("amount", "INTEGER", "YES")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "INTEGER", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        executor.applyTableChanges(
                Arrays.asList(
                        committable(
                                "s3://bucket/data-0.parquet",
                                "s3://bucket/keys-0.parquet",
                                0,
                                "0123456789abcdef"),
                        committable(
                                "s3://bucket/data-1.parquet",
                                "s3://bucket/keys-1.parquet",
                                1,
                                "fedcba9876543210")));

        String keyFiles =
                "read_parquet(['s3://bucket/keys-0.parquet', " + "'s3://bucket/keys-1.parquet'])";
        assertThat(client.allSql()).filteredOn(sql -> sql.contains(keyFiles)).hasSize(1);
        assertThat(client.executedSql)
                .anySatisfy(
                        sql -> assertThat(sql).startsWith("CREATE OR REPLACE TEMP TABLE \"keys_"));
    }

    @Test
    void preservesApplyFailureWhenTemporaryObjectCleanupAlsoFails() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO"), row("amount", "INTEGER", "YES")),
                        Arrays.asList(row("id", "BIGINT", "YES"), row("amount", "INTEGER", "YES")));
        client.failureSqlSubstring = "DELETE FROM";
        client.failureMessage = "injected operation failure";
        client.cleanupFailureSqlSubstring = "DROP TABLE IF EXISTS";
        DuckLakeJdbcCommitExecutor executor = executor(client);

        assertThatThrownBy(() -> executor.applyTableChanges(committable()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("injected operation failure")
                .satisfies(
                        failure ->
                                assertThat(failure.getSuppressed())
                                        .singleElement()
                                        .asString()
                                        .contains("injected cleanup failure"));
    }

    @Test
    void fencesEveryCheckpointTransactionBeforeApplyingChanges() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        executor.beginCheckpoint(
                "sink",
                "operator",
                Arrays.asList(
                        TableId.tableId("sales", "orders"),
                        TableId.tableId("inventory", "customers")));

        assertThat(client.executedSql)
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "UPDATE \"ducklake\".\"_flink_cdc_internal\".\"coordination\" "
                                + "SET lock_version = lock_version + 1 "
                                + "WHERE sink_instance_id = 'sink' AND operator_id = 'operator'",
                        "UPDATE \"ducklake\".\"_flink_cdc_internal\".\"coordination\" "
                                + "SET lock_version = lock_version + 1 "
                                + "WHERE sink_instance_id = '_flink_cdc_table' "
                                + "AND operator_id = '5:sales6:orders'",
                        "UPDATE \"ducklake\".\"_flink_cdc_internal\".\"coordination\" "
                                + "SET lock_version = lock_version + 1 "
                                + "WHERE sink_instance_id = '_flink_cdc_table' "
                                + "AND operator_id = '9:inventory9:customers'");
    }

    @Test
    void deduplicatesCaseVariantsOfTheSameDuckDbTableFence() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        executor.beginCheckpoint(
                "sink",
                "operator",
                Arrays.asList(
                        TableId.tableId("Sales", "Orders"), TableId.tableId("sales", "orders")));

        assertThat(client.executedSql)
                .filteredOn(
                        sql ->
                                sql.contains("sink_instance_id = '_flink_cdc_table'")
                                        && sql.contains("operator_id = '5:sales6:orders'"))
                .hasSize(1);
    }

    @Test
    void rechecksMissingCoordinationRowAfterAcquiringGlobalFence() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES")));
        client.coordinationRowExists = false;
        client.simulateConcurrentCoordinationInsert = true;
        DuckLakeJdbcCommitExecutor executor = executor(client);

        executor.beginCheckpoint("sink", "operator", Collections.emptyList());

        assertThat(client.coordinationRowQueries).isEqualTo(2);
        assertThat(client.executedSql)
                .noneSatisfy(
                        sql ->
                                assertThat(sql)
                                        .startsWith(
                                                "INSERT INTO \"ducklake\".\"_flink_cdc_internal\".\"coordination\" "
                                                        + "(sink_instance_id, operator_id, lock_version) VALUES ('sink'"));
    }

    @Test
    void rollsBackWhenAcquiringTableFenceFailsAfterBegin() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES")));
        client.failureSqlSubstring = "operator_id = '5:sales6:orders'";
        DuckLakeJdbcCommitExecutor executor = executor(client);

        assertThatThrownBy(
                        () ->
                                executor.beginCheckpoint(
                                        "sink",
                                        "operator",
                                        Collections.singletonList(
                                                TableId.tableId("sales", "orders"))))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("injected table fence failure");
        assertThat(client.executedSql.get(client.executedSql.size() - 1)).isEqualTo("ROLLBACK");
    }

    @Test
    void initializesCoordinationRowAtomicallyWithItsTable() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES")));
        client.coordinationTableExists = false;
        DuckLakeJdbcCommitExecutor executor = uninitializedExecutor(client);

        executor.findCommittedPlanHash("sink", "operator", 1);

        assertThat(client.executedSql)
                .containsSubsequence(
                        "BEGIN TRANSACTION",
                        "CREATE SCHEMA IF NOT EXISTS \"ducklake\".\"_flink_cdc_internal\"",
                        "CREATE TABLE \"ducklake\".\"_flink_cdc_internal\".\"coordination\" "
                                + "(sink_instance_id VARCHAR NOT NULL, operator_id VARCHAR NOT NULL, "
                                + "lock_version BIGINT NOT NULL)",
                        "INSERT INTO \"ducklake\".\"_flink_cdc_internal\".\"coordination\" "
                                + "(sink_instance_id, operator_id, lock_version) VALUES "
                                + "('_flink_cdc_global', '_flink_cdc_global', 0)",
                        "COMMIT");
    }

    @Test
    void recordsCheckpointIdentityOnDuckLakeSnapshot() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        executor.recordCheckpoint("sink", "operator", 42, "plan-hash");

        assertThat(client.executedSql)
                .first()
                .asString()
                .contains("\"ducklake\".set_commit_message")
                .contains("Flink CDC checkpoint 42")
                .contains("primary-key-current-state")
                .contains("plan-hash");
        assertThat(client.executedSql.get(1))
                .startsWith("INSERT INTO \"ducklake\".\"_flink_cdc_internal\".\"commits\"");
    }

    @Test
    void cleansOnlyOrphanFilesOlderThanRetentionWindow() throws Exception {
        TestingCatalogOperations client =
                new TestingCatalogOperations(
                        Arrays.asList(row("id", "BIGINT", "NO")),
                        Arrays.asList(row("id", "BIGINT", "YES")));
        DuckLakeJdbcCommitExecutor executor = executor(client);

        executor.cleanupOrphanFiles(Duration.ofDays(7));

        assertThat(client.executedSql)
                .containsExactly(
                        "CALL ducklake_delete_orphaned_files('ducklake', "
                                + "older_than => current_timestamp - INTERVAL '604800 seconds')");
    }

    private static DuckLakeJdbcCommitExecutor executor(DuckLakeCatalogOperations client) {
        return executor(client, true);
    }

    private static DuckLakeJdbcCommitExecutor uninitializedExecutor(
            DuckLakeCatalogOperations client) {
        return executor(client, false);
    }

    private static DuckLakeJdbcCommitExecutor executor(
            DuckLakeCatalogOperations client, boolean initialized) {
        return new DuckLakeJdbcCommitExecutor(client, initialized);
    }

    private static DuckLakeCommittable committable() {
        return committable("s3://bucket/data.parquet", 1);
    }

    private static DuckLakeCommittable keyOnlyCommittable() {
        return committable(null, 0);
    }

    private static DuckLakeCommittable committable(String dataFilePath, long dataRowCount) {
        return committable(
                dataFilePath, "s3://bucket/keys.parquet", 0, "0123456789abcdef", dataRowCount);
    }

    private static DuckLakeCommittable committable(
            String dataFilePath, String keyFilePath, int subtaskId, String hash) {
        return committable(dataFilePath, keyFilePath, subtaskId, hash, 1);
    }

    private static DuckLakeCommittable committable(
            String dataFilePath,
            String keyFilePath,
            int subtaskId,
            String hash,
            long dataRowCount) {
        DuckLakeWriteResult writeResult =
                new DuckLakeWriteResult(
                        TableId.tableId("sales", "orders"),
                        1,
                        0,
                        subtaskId,
                        0,
                        0,
                        Arrays.asList(
                                new DuckLakeColumnMetadata("id", DataTypes.BIGINT().notNull()),
                                new DuckLakeColumnMetadata("amount", DataTypes.INT())),
                        Collections.singletonList("id"),
                        dataFilePath,
                        dataRowCount,
                        keyFilePath,
                        1);
        return new DuckLakeCommittable("sink", "operator", writeResult, hash);
    }

    private static List<String> row(String name, String type, String nullable) {
        return Arrays.asList(name, type, nullable);
    }

    private static final class TestingCatalogOperations implements DuckLakeCatalogOperations {
        private final List<List<String>> targetSchema;
        private final List<List<String>> fileSchema;
        private final List<List<String>> keySchema;
        private final List<String> executedSql = new java.util.ArrayList<>();
        private final List<String> queriedSql = new java.util.ArrayList<>();
        private boolean coordinationTableExists = true;
        private boolean coordinationRowExists = true;
        private boolean simulateConcurrentCoordinationInsert;
        private String failureSqlSubstring;
        private String failureMessage = "injected table fence failure";
        private String cleanupFailureSqlSubstring;
        private int coordinationRowQueries;

        private TestingCatalogOperations(
                List<List<String>> targetSchema, List<List<String>> fileSchema) {
            this(targetSchema, fileSchema, Collections.singletonList(row("id", "BIGINT", "YES")));
        }

        private TestingCatalogOperations(
                List<List<String>> targetSchema,
                List<List<String>> fileSchema,
                List<List<String>> keySchema) {
            this.targetSchema = targetSchema;
            this.fileSchema = fileSchema;
            this.keySchema = keySchema;
        }

        @Override
        public void execute(String sql) throws SQLException {
            executedSql.add(sql);
            if (cleanupFailureSqlSubstring != null && sql.contains(cleanupFailureSqlSubstring)) {
                throw new SQLException("injected cleanup failure", "58000");
            }
            if (failureSqlSubstring != null && sql.contains(failureSqlSubstring)) {
                throw new SQLException(failureMessage, "40001");
            }
            if (simulateConcurrentCoordinationInsert
                    && sql.contains("SET lock_version = lock_version + 1")
                    && sql.contains("'_flink_cdc_global'")) {
                coordinationRowExists = true;
                simulateConcurrentCoordinationInsert = false;
            }
        }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> rowMapper) throws SQLException {
            queriedSql.add(sql);
            if (sql.startsWith("SELECT table_name FROM information_schema.tables")) {
                if (!coordinationTableExists) {
                    return Collections.emptyList();
                }
                return Collections.singletonList(
                        rowMapper.map(resultSet(Collections.singletonList("coordination"))));
            }
            if (sql.startsWith("SELECT plan_hash FROM")) {
                return Collections.emptyList();
            }
            if (sql.startsWith("SELECT 1 FROM read_parquet")
                    || sql.startsWith("SELECT 1 FROM \"keys_")) {
                return Collections.emptyList();
            }
            if (sql.startsWith("SELECT lock_version FROM")) {
                if (sql.contains("sink_instance_id = 'sink'")) {
                    coordinationRowQueries++;
                    if (!coordinationRowExists) {
                        return Collections.emptyList();
                    }
                }
                return Collections.singletonList(
                        rowMapper.map(resultSet(Collections.singletonList("0"))));
            }
            List<List<String>> rows =
                    sql.contains("keys.parquet")
                                    || sql.contains("keys-")
                                    || sql.startsWith("DESCRIBE SELECT * FROM \"keys_")
                            ? keySchema
                            : sql.contains("read_parquet") ? fileSchema : targetSchema;
            java.util.ArrayList<T> result = new java.util.ArrayList<>();
            for (List<String> row : rows) {
                result.add(rowMapper.map(resultSet(row)));
            }
            return result;
        }

        @Override
        public void close() {}

        private List<String> allSql() {
            java.util.ArrayList<String> sql = new java.util.ArrayList<>(executedSql);
            sql.addAll(queriedSql);
            return sql;
        }
    }

    private static ResultSet resultSet(List<String> row) {
        return (ResultSet)
                Proxy.newProxyInstance(
                        DuckLakeJdbcCommitExecutorTest.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getString")) {
                                return row.get((int) args[0] - 1);
                            }
                            if (method.getName().equals("getLong")) {
                                return Long.parseLong(row.get((int) args[0] - 1));
                            }
                            return null;
                        });
    }
}
