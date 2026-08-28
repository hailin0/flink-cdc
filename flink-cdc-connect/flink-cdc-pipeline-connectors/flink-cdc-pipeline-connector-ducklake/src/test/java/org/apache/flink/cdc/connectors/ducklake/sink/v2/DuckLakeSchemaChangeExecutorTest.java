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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogOperations;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeSchemaChangeExecutorTest {

    private static final TableId ORDERS = TableId.tableId("sales", "orders");

    @Test
    void appliesSupportedChangesIdempotentlyAfterCreate() throws Exception {
        RecordingCatalog catalog = new RecordingCatalog();
        DuckLakeSchemaChangeExecutor executor = new DuckLakeSchemaChangeExecutor(catalog);
        executor.apply(new CreateTableEvent(ORDERS, schema()));
        catalog.statements.clear();

        AddColumnEvent addColumn =
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("extra", DataTypes.STRING()))));
        AlterColumnTypeEvent alterColumn = alterType("amount", DataTypes.INT(), DataTypes.BIGINT());
        RenameColumnEvent renameColumn =
                new RenameColumnEvent(ORDERS, Collections.singletonMap("description", "details"));
        DropColumnEvent dropColumn =
                new DropColumnEvent(ORDERS, Collections.singletonList("extra"));

        executor.apply(addColumn);
        executor.apply(addColumn);
        executor.apply(alterColumn);
        executor.apply(alterColumn);
        executor.apply(renameColumn);
        executor.apply(renameColumn);
        executor.apply(dropColumn);
        executor.apply(dropColumn);

        assertThat(catalog.statements)
                .containsExactly(
                        "ALTER TABLE \"ducklake\".\"sales\".\"orders\" "
                                + "ADD COLUMN \"extra\" VARCHAR",
                        "ALTER TABLE \"ducklake\".\"sales\".\"orders\" "
                                + "ALTER COLUMN \"amount\" SET TYPE BIGINT",
                        "ALTER TABLE \"ducklake\".\"sales\".\"orders\" "
                                + "ADD COLUMN \"details\" VARCHAR",
                        "UPDATE \"ducklake\".\"sales\".\"orders\" "
                                + "SET \"details\" = \"description\"",
                        "ALTER TABLE \"ducklake\".\"sales\".\"orders\" "
                                + "ALTER COLUMN \"details\" SET NOT NULL",
                        "ALTER TABLE \"ducklake\".\"sales\".\"orders\" "
                                + "DROP COLUMN \"description\"",
                        "INSERT INTO \"ducklake\".\"_flink_cdc_internal\".\"dropped_columns\" "
                                + "(schema_name, table_name, column_name) VALUES "
                                + "('sales', 'orders', 'description')",
                        "ALTER TABLE \"ducklake\".\"sales\".\"orders\" " + "DROP COLUMN \"extra\"",
                        "INSERT INTO \"ducklake\".\"_flink_cdc_internal\".\"dropped_columns\" "
                                + "(schema_name, table_name, column_name) VALUES "
                                + "('sales', 'orders', 'extra')");

        assertThatThrownBy(
                        () ->
                                executor.apply(
                                        new AddColumnEvent(
                                                ORDERS,
                                                Collections.singletonList(
                                                        AddColumnEvent.last(
                                                                Column.physicalColumn(
                                                                        "description",
                                                                        DataTypes.STRING()))))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "Reusing a dropped DuckLake column name is not supported: description.");
    }

    @Test
    void rejectsPrimaryKeyChanges() throws Exception {
        RecordingCatalog catalog = new RecordingCatalog();
        DuckLakeSchemaChangeExecutor executor = new DuckLakeSchemaChangeExecutor(catalog);
        executor.apply(new CreateTableEvent(ORDERS, schema()));
        catalog.statements.clear();

        assertThatThrownBy(
                        () ->
                                executor.apply(
                                        new RenameColumnEvent(
                                                ORDERS,
                                                Collections.singletonMap("id", "order_id"))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Renaming a primary-key column is not supported.");
        assertThatThrownBy(
                        () ->
                                executor.apply(
                                        new DropColumnEvent(
                                                ORDERS, Collections.singletonList("id"))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Dropping a primary-key column is not supported.");
        assertThatThrownBy(
                        () ->
                                executor.apply(
                                        alterType(
                                                "id",
                                                DataTypes.BIGINT(),
                                                DataTypes.DECIMAL(20, 0))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Changing a primary-key column is not supported.");
        assertThat(catalog.statements).isEmpty();
    }

    @Test
    void rejectsRenameToExistingColumn() throws Exception {
        RecordingCatalog catalog = new RecordingCatalog();
        DuckLakeSchemaChangeExecutor executor = new DuckLakeSchemaChangeExecutor(catalog);
        executor.apply(new CreateTableEvent(ORDERS, schema()));
        catalog.statements.clear();

        assertThatThrownBy(
                        () ->
                                executor.apply(
                                        new RenameColumnEvent(
                                                ORDERS,
                                                Collections.singletonMap("description", "amount"))))
                .isInstanceOf(SchemaEvolveException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Rename target column already exists: amount.");
        assertThat(catalog.statements).isEmpty();
    }

    private static Schema schema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("amount", DataTypes.INT())
                .physicalColumn("description", DataTypes.STRING().notNull())
                .primaryKey("id")
                .build();
    }

    private static AlterColumnTypeEvent alterType(
            String columnName, DataType oldType, DataType newType) {
        Map<String, DataType> oldTypes = new LinkedHashMap<>();
        oldTypes.put(columnName, oldType);
        Map<String, DataType> newTypes = new LinkedHashMap<>();
        newTypes.put(columnName, newType);
        return new AlterColumnTypeEvent(ORDERS, newTypes, oldTypes);
    }

    private static final class RecordingCatalog implements DuckLakeCatalogOperations {
        private final List<String> statements = new ArrayList<>();

        @Override
        public void execute(String sql) {
            statements.add(sql);
        }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
            return Collections.emptyList();
        }

        @Override
        public void close() throws SQLException {}
    }
}
