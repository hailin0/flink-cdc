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
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeMetadataApplierTest {

    private static final TableId ORDERS = TableId.tableId("sales", "orders");

    private DuckLakeMetadataApplier applier;

    @BeforeEach
    void setUp() {
        applier = new DuckLakeMetadataApplier();
    }

    @Test
    void validatesSupportedChanges() {
        applier.applySchemaChange(new CreateTableEvent(ORDERS, primaryKeySchema()));
        applier.applySchemaChange(
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("extra", DataTypes.STRING())))));
        applier.applySchemaChange(new DropColumnEvent(ORDERS, Collections.singletonList("extra")));
        applier.applySchemaChange(
                new RenameColumnEvent(ORDERS, Collections.singletonMap("description", "details")));
        applier.applySchemaChange(alterType("amount", DataTypes.INT(), DataTypes.BIGINT()));
    }

    @Test
    void reportsSupportedSchemaEvolutionTypes() {
        assertThat(applier.getSupportedSchemaEvolutionTypes())
                .containsExactlyInAnyOrder(
                        SchemaChangeEventType.CREATE_TABLE,
                        SchemaChangeEventType.ADD_COLUMN,
                        SchemaChangeEventType.DROP_COLUMN,
                        SchemaChangeEventType.RENAME_COLUMN,
                        SchemaChangeEventType.ALTER_COLUMN_TYPE);
    }

    @Test
    void honorsAcceptedSchemaEvolutionTypes() {
        applier.setAcceptedSchemaEvolutionTypes(EnumSet.of(SchemaChangeEventType.CREATE_TABLE));

        assertThat(applier.acceptsSchemaEvolutionType(SchemaChangeEventType.CREATE_TABLE)).isTrue();
        assertThat(applier.acceptsSchemaEvolutionType(SchemaChangeEventType.ADD_COLUMN)).isFalse();
        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new DropColumnEvent(
                                                ORDERS, Collections.singletonList("amount"))))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidPrimaryKeySchemas")
    void rejectsInvalidPrimaryKeySchema(String testCase, Schema schema) {
        assertThatThrownBy(() -> applier.applySchemaChange(new CreateTableEvent(ORDERS, schema)))
                .isInstanceOf(SchemaEvolveException.class);
    }

    @Test
    void rejectsUnsafeAddColumnDefinitions() {
        AddColumnEvent nonLast =
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.after(
                                        Column.physicalColumn("extra", DataTypes.STRING()),
                                        "amount")));
        AddColumnEvent notNull =
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "extra", DataTypes.STRING().notNull()))));

        assertThatThrownBy(() -> applier.applySchemaChange(nonLast))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "DuckLake cannot preserve non-LAST column positions.");
        assertThatThrownBy(() -> applier.applySchemaChange(notNull))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "Adding a NOT NULL column without a proven literal default is unsafe.");
    }

    @Test
    void rejectsUnsafeAlterColumnTypeDefinitions() {
        AlterColumnTypeEvent missingOldType =
                new AlterColumnTypeEvent(
                        ORDERS, Collections.singletonMap("amount", DataTypes.BIGINT()));
        AlterColumnTypeEvent narrowing = alterType("amount", DataTypes.BIGINT(), DataTypes.INT());

        assertThatThrownBy(() -> applier.applySchemaChange(missingOldType))
                .isInstanceOf(SchemaEvolveException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Missing or stale pre-schema type for column amount.");
        assertThatThrownBy(() -> applier.applySchemaChange(narrowing))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "DuckLake does not support the requested lossless type promotion: "
                                + "BIGINT -> INT");
    }

    @Test
    void rejectsInvalidRenameMapping() {
        Map<String, String> duplicateTargets = new LinkedHashMap<>();
        duplicateTargets.put("amount", "value");
        duplicateTargets.put("description", "value");
        Map<String, String> cyclicRename = new LinkedHashMap<>();
        cyclicRename.put("amount", "description");
        cyclicRename.put("description", "amount");

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new RenameColumnEvent(ORDERS, Collections.emptyMap())))
                .isInstanceOf(SchemaEvolveException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Rename column mapping must not be empty.");
        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new RenameColumnEvent(ORDERS, duplicateTargets)))
                .isInstanceOf(SchemaEvolveException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Rename column targets must be unique.");

        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new RenameColumnEvent(ORDERS, cyclicRename)))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage", "Chained or cyclic column renames are not supported.");
    }

    @Test
    void rejectsUnsupportedAndInvalidTargetIdentifiers() {
        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new AlterTableCommentEvent(ORDERS, "comment")))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class);
        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(
                                                TableId.tableId("catalog", "sales", "orders"),
                                                primaryKeySchema())))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "Three-part table identifiers must be routed to a two-part target identifier.");
        assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(
                                                TableId.tableId("_flink_cdc_internal", "orders"),
                                                primaryKeySchema())))
                .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                .hasFieldOrPropertyWithValue(
                        "exceptionMessage",
                        "Target schema _flink_cdc_internal is reserved by the DuckLake connector.");
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
                .physicalColumn("description", DataTypes.STRING())
                .primaryKey("id")
                .build();
    }

    private static AlterColumnTypeEvent alterType(
            String column, DataType oldType, DataType newType) {
        Map<String, DataType> oldTypes = new LinkedHashMap<>();
        oldTypes.put(column, oldType);
        Map<String, DataType> newTypes = new LinkedHashMap<>();
        newTypes.put(column, newType);
        return new AlterColumnTypeEvent(ORDERS, newTypes, oldTypes);
    }
}
