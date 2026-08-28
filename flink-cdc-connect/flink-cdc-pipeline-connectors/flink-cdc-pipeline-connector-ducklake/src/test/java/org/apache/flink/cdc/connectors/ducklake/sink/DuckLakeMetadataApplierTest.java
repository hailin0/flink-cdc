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

package org.apache.flink.cdc.connectors.ducklake.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.ducklake.testutils.JdbcTestUtils.defaultValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DuckLakeMetadataApplier}. */
class DuckLakeMetadataApplierTest {

    private static final TableId ORDERS = TableId.tableId("sales", "order");

    private List<String> statements;
    private DuckLakeMetadataApplier applier;

    @BeforeEach
    void setUp() {
        statements = new ArrayList<>();
        applier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(recordingConnection(statements, null)));
    }

    @Test
    void createsTableRequiredByCdcPrimaryKeyWithoutUnsupportedTargetConstraint() {
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));

        assertThat(statements)
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "CREATE SCHEMA IF NOT EXISTS \"ducklake\".\"sales\"",
                        "CREATE SCHEMA IF NOT EXISTS \"ducklake\".\"_flink_cdc_internal\"",
                        "CREATE TABLE IF NOT EXISTS "
                                + "\"ducklake\".\"_flink_cdc_internal\".\"source_primary_keys\" "
                                + "(schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
                                + "key_order INTEGER NOT NULL, column_name VARCHAR NOT NULL)",
                        "CREATE TABLE IF NOT EXISTS "
                                + "\"ducklake\".\"_flink_cdc_internal\".\"dropped_columns\" "
                                + "(schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
                                + "column_name VARCHAR NOT NULL)",
                        "CREATE TABLE \"ducklake\".\"sales\".\"order\" "
                                + "(\"id\" BIGINT NOT NULL, \"amount\" INTEGER, \"note\" VARCHAR)",
                        "INSERT INTO "
                                + "\"ducklake\".\"_flink_cdc_internal\".\"source_primary_keys\" "
                                + "(schema_name, table_name, key_order, column_name) "
                                + "VALUES ('sales', 'order', 0, 'id')",
                        "COMMIT");
        assertThat(statements).noneMatch(sql -> sql.contains("PRIMARY KEY"));
    }

    @Test
    void addsAndDropsColumnsInSeparateTransactions() {
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        statements.clear();

        applier.applySchemaChange(
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("extra", DataTypes.STRING())))));
        applier.applySchemaChange(new DropColumnEvent(ORDERS, Collections.singletonList("extra")));

        assertThat(statements)
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ADD COLUMN \"extra\" VARCHAR",
                        "COMMIT",
                        "BEGIN TRANSACTION",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" " + "DROP COLUMN \"extra\"",
                        "INSERT INTO "
                                + "\"ducklake\".\"_flink_cdc_internal\".\"dropped_columns\" "
                                + "(schema_name, table_name, column_name) "
                                + "VALUES ('sales', 'order', 'extra')",
                        "COMMIT");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("replayableColumnChanges")
    void treatsIdenticalColumnChangeReplayAsNoOp(String testCase, SchemaChangeEvent schemaChange) {
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        applier.applySchemaChange(schemaChange);
        statements.clear();

        applier.applySchemaChange(schemaChange);

        assertThat(statements).containsExactly("BEGIN TRANSACTION", "COMMIT");
    }

    @Test
    void rejectsReusingDroppedColumnName() {
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        applier.applySchemaChange(new DropColumnEvent(ORDERS, Collections.singletonList("note")));
        statements.clear();

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new AddColumnEvent(
                                                ORDERS,
                                                Collections.singletonList(
                                                        AddColumnEvent.last(
                                                                Column.physicalColumn(
                                                                        "note",
                                                                        DataTypes.STRING()))))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "Reusing a dropped DuckLake column name is not supported: note.");
        assertThat(statements).isEmpty();
    }

    @Test
    void rejectsReusingDroppedColumnNameAfterRecovery() {
        applier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(recoveringConnection(statements)));

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new AddColumnEvent(
                                                ORDERS,
                                                Collections.singletonList(
                                                        AddColumnEvent.last(
                                                                Column.physicalColumn(
                                                                        "note",
                                                                        DataTypes.STRING()))))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "Reusing a dropped DuckLake column name is not supported: note.");
        assertThat(statements).isEmpty();
    }

    @Test
    void treatsIdenticalCreateTableReplayAsNoOp() {
        applier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(recoveringConnection(statements)));

        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));

        assertThat(statements).containsExactly("BEGIN TRANSACTION", "COMMIT");
    }

    @Test
    void inspectsDuckLakeTableThroughGlobalInformationSchema() {
        List<String> queries = new ArrayList<>();
        applier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(recoveringConnection(statements, queries::add)));

        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));

        assertThat(queries.get(0))
                .isEqualTo(
                        "SELECT table_name FROM information_schema.tables"
                                + " WHERE table_catalog = 'ducklake'"
                                + " AND table_schema = 'sales' AND table_name = 'order'");
    }

    @Test
    void rejectsConflictingCreateTableReplay() {
        applier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(recoveringConnection(statements)));
        Schema conflicting =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("amount", DataTypes.STRING())
                        .physicalColumn("note", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        assertThatThrownBy(
                        () -> applier.applySchemaChange(new CreateTableEvent(ORDERS, conflicting)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "Conflicting DuckLake table already exists: sales.order");
        assertThat(statements).isEmpty();
    }

    @Test
    void addsColumnAfterRecoveryWithoutReplayedCreateEvent() {
        applier.applySchemaChange(
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("extra", DataTypes.STRING())))));

        assertThat(statements)
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ADD COLUMN \"extra\" VARCHAR",
                        "COMMIT");
    }

    @Test
    void protectsPrimaryKeyAfterRecoveryUsingCatalogMetadata() {
        applier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(recoveringConnection(statements)));

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new DropColumnEvent(
                                                ORDERS, Collections.singletonList("id"))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Dropping a primary-key column is not supported.");
    }

    @Test
    void promotesColumnAfterRecoveryUsingCatalogSchema() {
        applier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(recoveringConnection(statements)));

        applier.applySchemaChange(alterType("amount", DataTypes.INT(), DataTypes.BIGINT()));

        assertThat(statements)
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ALTER COLUMN \"amount\" SET TYPE BIGINT",
                        "COMMIT");
    }

    @Test
    void promotesIntegerToBigint() {
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        statements.clear();
        applier.applySchemaChange(alterType("amount", DataTypes.INT(), DataTypes.BIGINT()));

        assertThat(statements)
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ALTER COLUMN \"amount\" SET TYPE BIGINT",
                        "COMMIT");
    }

    @Test
    void appliesNullabilityChangeTogetherWithTypePromotion() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("amount", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(ORDERS, schema));
        statements.clear();

        applier.applySchemaChange(
                alterType("amount", DataTypes.INT().notNull(), DataTypes.BIGINT()));

        assertThat(statements)
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ALTER COLUMN \"amount\" SET TYPE BIGINT",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ALTER COLUMN \"amount\" DROP NOT NULL",
                        "COMMIT");
    }

    @Test
    void appliesNullabilityOnlyAlteration() {
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        statements.clear();

        applier.applySchemaChange(alterType("amount", DataTypes.INT(), DataTypes.INT().notNull()));

        assertThat(statements)
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ALTER COLUMN \"amount\" SET NOT NULL",
                        "COMMIT");
    }

    @ParameterizedTest
    @MethodSource("unsupportedEvents")
    void rejectsUnsafeSchemaChangesBeforeOpeningTransaction(SchemaChangeEvent event) {
        assertThatThrownBy(() -> applier.applySchemaChange(event))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class);

        assertThat(statements).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidPrimaryKeySchemas")
    void rejectsInvalidPrimaryKeyBeforeClientExecution(String testCase, Schema schema) {
        CreateTableEvent event = new CreateTableEvent(ORDERS, schema);

        assertThatThrownBy(() -> applier.applySchemaChange(event))
                .isInstanceOf(SchemaEvolveException.class)
                .hasFieldOrPropertyWithValue("applyingEvent", event);
        assertThat(statements).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "_flink_cdc_internal",
                "_FLINK_CDC_INTERNAL",
                "_flink_cdc_staging",
                "_FLINK_CDC_STAGING"
            })
    void rejectsConnectorOwnedSchemaBeforeClientExecution(String schemaName) {
        TableId reserved = TableId.tableId(schemaName, "commits");

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(reserved, primaryKeySchema())))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "Target schema "
                                + schemaName.toLowerCase(java.util.Locale.ROOT)
                                + " is reserved by the DuckLake connector.");
        assertThat(statements).isEmpty();
    }

    @Test
    void rejectsPrimaryKeyChanges() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("amount", DataTypes.DECIMAL(10, 2))
                        .primaryKey("id")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(ORDERS, schema));
        statements.clear();

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        alterType(
                                                "id",
                                                DataTypes.INT().notNull(),
                                                DataTypes.BIGINT().notNull())))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Changing a primary-key column is not supported.");
        assertThat(statements).isEmpty();
    }

    @Test
    void rejectsUnprovenTypePromotion() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("amount", DataTypes.DECIMAL(10, 2))
                        .primaryKey("id")
                        .build();
        applier.applySchemaChange(new CreateTableEvent(ORDERS, schema));
        statements.clear();

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        alterType(
                                                "amount",
                                                DataTypes.DECIMAL(10, 2),
                                                DataTypes.DECIMAL(12, 2))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class);
        assertThat(statements).isEmpty();
    }

    @Test
    void rejectsColumnPositionsThatDuckLakeCannotPreserve() {
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        statements.clear();

        AddColumnEvent positionedAdd =
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.after(
                                        Column.physicalColumn("extra", DataTypes.STRING()),
                                        "amount")));

        assertThatThrownBy(() -> applier.applySchemaChange(positionedAdd))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "DuckLake cannot preserve non-LAST column positions.");
        assertThat(statements).isEmpty();
    }

    @Test
    void rollsBackFailedDdl() {
        List<String> failedStatements = new ArrayList<>();
        DuckLakeMetadataApplier failingApplier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(
                                recordingConnection(failedStatements, "CREATE TABLE")));

        assertThatThrownBy(
                        () ->
                                failingApplier.applySchemaChange(
                                        new CreateTableEvent(ORDERS, primaryKeySchema())))
                .isInstanceOf(SchemaEvolveException.class)
                .hasCauseInstanceOf(SQLException.class);
        assertThat(failedStatements.get(failedStatements.size() - 1)).isEqualTo("ROLLBACK");
    }

    @Test
    void locksTableBeforeApplyingDdl() {
        applier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(recordingConnection(statements, null)),
                        0,
                        Duration.ZERO);
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        statements.clear();

        applier.applySchemaChange(
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("extra", DataTypes.STRING())))));

        assertThat(statements.subList(statements.size() - 4, statements.size()))
                .containsExactly(
                        "BEGIN TRANSACTION",
                        "UPDATE \"ducklake\".\"_flink_cdc_internal\".\"coordination\" "
                                + "SET lock_version = lock_version + 1 "
                                + "WHERE sink_instance_id = '_flink_cdc_table' "
                                + "AND operator_id = '5:sales5:order'",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ADD COLUMN \"extra\" VARCHAR",
                        "COMMIT");
    }

    @Test
    void retriesRetryableDdlConflict() {
        AtomicInteger failures = new AtomicInteger(1);
        DuckLakeMetadataApplier retryingApplier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(
                                retryingConnection(statements, "ALTER TABLE", failures)),
                        1,
                        Duration.ZERO);
        retryingApplier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        statements.clear();

        retryingApplier.applySchemaChange(
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("extra", DataTypes.STRING())))));

        assertThat(failures).hasValue(0);
        assertThat(statements)
                .containsSubsequence(
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ADD COLUMN \"extra\" VARCHAR",
                        "ROLLBACK",
                        "BEGIN TRANSACTION",
                        "ALTER TABLE \"ducklake\".\"sales\".\"order\" "
                                + "ADD COLUMN \"extra\" VARCHAR",
                        "COMMIT");
    }

    @Test
    void rebuildsDdlPlanAfterConcurrentTransactionAppliesTheChange() {
        AtomicInteger addColumnAttempts = new AtomicInteger();
        DuckLakeMetadataApplier retryingApplier =
                new DuckLakeMetadataApplier(
                        new DuckLakeCatalogClient(
                                concurrentAddColumnConnection(statements, addColumnAttempts)),
                        1,
                        Duration.ZERO);
        retryingApplier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        statements.clear();

        retryingApplier.applySchemaChange(
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("extra", DataTypes.STRING())))));

        assertThat(addColumnAttempts).hasValue(1);
        assertThat(statements).contains("ROLLBACK");
        assertThat(statements).filteredOn(sql -> sql.contains("ADD COLUMN \"extra\"")).hasSize(1);
    }

    private static Stream<SchemaChangeEvent> unsupportedEvents() {
        return Stream.of(
                new RenameColumnEvent(ORDERS, Collections.singletonMap("note", "memo")),
                new TruncateTableEvent(ORDERS),
                new DropTableEvent(ORDERS),
                new AlterTableCommentEvent(ORDERS, "unsafe with current checkpoint ordering"));
    }

    private static Stream<Arguments> replayableColumnChanges() {
        return Stream.of(
                Arguments.of(
                        "ADD COLUMN replay",
                        new AddColumnEvent(
                                ORDERS,
                                Collections.singletonList(
                                        AddColumnEvent.last(
                                                Column.physicalColumn(
                                                        "extra", DataTypes.STRING()))))),
                Arguments.of(
                        "DROP COLUMN replay",
                        new DropColumnEvent(ORDERS, Collections.singletonList("note"))),
                Arguments.of(
                        "ALTER COLUMN replay",
                        alterType("amount", DataTypes.INT(), DataTypes.BIGINT())));
    }

    private static Stream<Arguments> invalidPrimaryKeySchemas() {
        return Stream.of(
                Arguments.of(
                        "missing primary key",
                        Schema.newBuilder().physicalColumn("id", DataTypes.BIGINT()).build()),
                Arguments.of(
                        "nullable primary key",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.BIGINT())
                                .primaryKey("id")
                                .build()));
    }

    private static Schema primaryKeySchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("amount", DataTypes.INT())
                .physicalColumn("note", DataTypes.STRING())
                .primaryKey("id")
                .build();
    }

    private static AlterColumnTypeEvent alterType(
            String column,
            org.apache.flink.cdc.common.types.DataType oldType,
            org.apache.flink.cdc.common.types.DataType newType) {
        Map<String, org.apache.flink.cdc.common.types.DataType> oldTypes = new LinkedHashMap<>();
        oldTypes.put(column, oldType);
        Map<String, org.apache.flink.cdc.common.types.DataType> newTypes = new LinkedHashMap<>();
        newTypes.put(column, newType);
        return new AlterColumnTypeEvent(ORDERS, newTypes, oldTypes);
    }

    private static Connection recordingConnection(
            List<String> statements, String failingSqlPrefix) {
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                DuckLakeMetadataApplierTest.class.getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("execute")) {
                                        String sql = (String) args[0];
                                        statements.add(sql);
                                        if (failingSqlPrefix != null
                                                && sql.startsWith(failingSqlPrefix)) {
                                            throw new SQLException("injected DDL failure");
                                        }
                                        return true;
                                    }
                                    if (method.getName().equals("executeQuery")) {
                                        return resultSet(Collections.emptyList());
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        return (Connection)
                Proxy.newProxyInstance(
                        DuckLakeMetadataApplierTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("createStatement")) {
                                return statement;
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private static Connection retryingConnection(
            List<String> statements, String failingSqlPrefix, AtomicInteger failures) {
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                DuckLakeMetadataApplierTest.class.getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("execute")) {
                                        String sql = (String) args[0];
                                        statements.add(sql);
                                        if (sql.startsWith(failingSqlPrefix)
                                                && failures.get() > 0) {
                                            failures.decrementAndGet();
                                            throw new SQLException(
                                                    "serialization conflict", "40001");
                                        }
                                        return true;
                                    }
                                    if (method.getName().equals("executeQuery")) {
                                        return resultSet(Collections.emptyList());
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        return (Connection)
                Proxy.newProxyInstance(
                        DuckLakeMetadataApplierTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("createStatement")) {
                                return statement;
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private static Connection concurrentAddColumnConnection(
            List<String> statements, AtomicInteger addColumnAttempts) {
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                DuckLakeMetadataApplierTest.class.getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("execute")) {
                                        String sql = (String) args[0];
                                        statements.add(sql);
                                        if (sql.contains("ADD COLUMN \"extra\"")) {
                                            if (addColumnAttempts.getAndIncrement() == 0) {
                                                throw new SQLException(
                                                        "serialization conflict", "40001");
                                            }
                                            throw new SQLException("duplicate column", "42701");
                                        }
                                        return true;
                                    }
                                    if (method.getName().equals("executeQuery")) {
                                        String sql = (String) args[0];
                                        if (sql.startsWith("DESCRIBE")
                                                && addColumnAttempts.get() > 0) {
                                            return resultSet(
                                                    java.util.Arrays.asList(
                                                            java.util.Arrays.asList(
                                                                    "id", "BIGINT", "NO"),
                                                            java.util.Arrays.asList(
                                                                    "amount", "INTEGER", "YES"),
                                                            java.util.Arrays.asList(
                                                                    "note", "VARCHAR", "YES"),
                                                            java.util.Arrays.asList(
                                                                    "extra", "VARCHAR", "YES")));
                                        }
                                        return resultSet(Collections.emptyList());
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        return (Connection)
                Proxy.newProxyInstance(
                        DuckLakeMetadataApplierTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("createStatement")) {
                                return statement;
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private static Connection recoveringConnection(List<String> statements) {
        return recoveringConnection(statements, ignored -> {});
    }

    private static Connection recoveringConnection(
            List<String> statements, Consumer<String> queryConsumer) {
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                DuckLakeMetadataApplierTest.class.getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("execute")) {
                                        statements.add((String) args[0]);
                                        return true;
                                    }
                                    if (method.getName().equals("executeQuery")) {
                                        String sql = (String) args[0];
                                        queryConsumer.accept(sql);
                                        if (sql.contains("information_schema")
                                                && sql.contains("tables")) {
                                            return resultSet(
                                                    Collections.singletonList(
                                                            Collections.singletonList("order")));
                                        }
                                        if (sql.contains("dropped_columns")) {
                                            return resultSet(
                                                    Collections.singletonList(
                                                            Collections.singletonList("note")));
                                        }
                                        if (sql.contains("source_primary_keys")) {
                                            return resultSet(
                                                    Collections.singletonList(
                                                            Collections.singletonList("id")));
                                        }
                                        if (sql.startsWith("DESCRIBE")) {
                                            return resultSet(
                                                    java.util.Arrays.asList(
                                                            java.util.Arrays.asList(
                                                                    "id", "BIGINT", "NO"),
                                                            java.util.Arrays.asList(
                                                                    "amount", "INTEGER", "YES"),
                                                            java.util.Arrays.asList(
                                                                    "note", "VARCHAR", "YES")));
                                        }
                                        throw new SQLException("Unexpected query: " + sql);
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        return (Connection)
                Proxy.newProxyInstance(
                        DuckLakeMetadataApplierTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("createStatement")) {
                                return statement;
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private static ResultSet resultSet(List<List<String>> rows) {
        int[] rowIndex = {-1};
        return (ResultSet)
                Proxy.newProxyInstance(
                        DuckLakeMetadataApplierTest.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("next")) {
                                rowIndex[0]++;
                                return rowIndex[0] < rows.size();
                            }
                            if (method.getName().equals("getString")) {
                                return rows.get(rowIndex[0]).get((int) args[0] - 1);
                            }
                            return defaultValue(method.getReturnType());
                        });
    }
}
