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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.DuckLakeSchemaRepository.CatalogColumn;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeTypeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates a schema event and plans the idempotent DuckLake DDL statements for it. */
final class DuckLakeSchemaChangePlanner {

    private final DuckLakeSchemaRepository repository;

    DuckLakeSchemaChangePlanner(DuckLakeSchemaRepository repository) {
        this.repository = repository;
    }

    DuckLakeSchemaChangePlan plan(SchemaChangeEvent event) {
        return SchemaChangeEventVisitor.visit(
                event,
                this::planAddColumns,
                this::planAlterColumnTypes,
                this::planCreateTable,
                this::planDropColumns,
                unsupported -> {
                    throw new UnsupportedSchemaChangeEventException(unsupported);
                },
                unsupported -> {
                    throw new UnsupportedSchemaChangeEventException(unsupported);
                },
                unsupported -> {
                    throw new UnsupportedSchemaChangeEventException(unsupported);
                },
                unsupported -> {
                    throw new UnsupportedSchemaChangeEventException(unsupported);
                });
    }

    private DuckLakeSchemaChangePlan planCreateTable(CreateTableEvent event) {
        Schema schema = event.getSchema();
        validateCreateTable(event, schema);
        for (Column column : schema.getColumns()) {
            validateColumn(event, column);
        }
        Schema currentSchema = repository.currentSchema(event.tableId());
        if (currentSchema != null) {
            if (currentSchema.equals(schema)) {
                return new DuckLakeSchemaChangePlan(Collections.emptyList(), schema, null);
            }
            throw conflictingCreate(event);
        }
        if (repository.tableExists(event)) {
            validateExistingTable(event, schema);
            return new DuckLakeSchemaChangePlan(Collections.emptyList(), schema, null);
        }
        String schemaName = schemaName(event.tableId());
        List<String> statements = new ArrayList<>();
        statements.add(
                "CREATE SCHEMA IF NOT EXISTS "
                        + DuckDbSqlUtils.qualifiedName(
                                DuckLakeCatalogObjects.CATALOG_ALIAS, schemaName));
        statements.add(
                "CREATE SCHEMA IF NOT EXISTS "
                        + DuckDbSqlUtils.qualifiedName(
                                DuckLakeCatalogObjects.CATALOG_ALIAS,
                                DuckLakeCatalogObjects.INTERNAL_SCHEMA));
        statements.add(
                "CREATE TABLE IF NOT EXISTS "
                        + primaryKeyTable()
                        + " (schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
                        + "key_order INTEGER NOT NULL, column_name VARCHAR NOT NULL)");
        statements.add(
                "CREATE TABLE IF NOT EXISTS "
                        + droppedColumnTable()
                        + " (schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
                        + "column_name VARCHAR NOT NULL)");
        List<String> columnDefinitions = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            columnDefinitions.add(columnDefinition(column));
        }
        statements.add(
                "CREATE TABLE "
                        + tableName(event.tableId())
                        + " ("
                        + String.join(", ", columnDefinitions)
                        + ")");
        for (int i = 0; i < schema.primaryKeys().size(); i++) {
            statements.add(
                    "INSERT INTO "
                            + primaryKeyTable()
                            + " (schema_name, table_name, key_order, column_name) VALUES ("
                            + DuckDbSqlUtils.literal(schemaName)
                            + ", "
                            + DuckDbSqlUtils.literal(event.tableId().getTableName())
                            + ", "
                            + i
                            + ", "
                            + DuckDbSqlUtils.literal(schema.primaryKeys().get(i))
                            + ")");
        }
        return new DuckLakeSchemaChangePlan(statements, schema, Collections.emptySet());
    }

    private DuckLakeSchemaChangePlan planAddColumns(AddColumnEvent event) {
        Set<String> droppedColumns = repository.droppedColumns(event);
        Schema currentSchema = repository.currentSchema(event.tableId());
        List<CatalogColumn> catalogColumns =
                currentSchema == null
                        ? repository.loadCatalogColumns(event)
                        : Collections.emptyList();
        List<String> statements = new ArrayList<>();
        List<AddColumnEvent.ColumnWithPosition> unappliedColumns = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition addedColumn : event.getAddedColumns()) {
            if (addedColumn.getPosition() != AddColumnEvent.ColumnPosition.LAST) {
                throw new UnsupportedSchemaChangeEventException(
                        event, "DuckLake cannot preserve non-LAST column positions.");
            }
            Column column = addedColumn.getAddColumn();
            validateColumn(event, column);
            if (droppedColumns.contains(column.getName())) {
                throw new UnsupportedSchemaChangeEventException(
                        event,
                        "Reusing a dropped DuckLake column name is not supported: "
                                + column.getName()
                                + ".");
            }
            if (!column.getType().isNullable()) {
                throw new UnsupportedSchemaChangeEventException(
                        event,
                        "Adding a NOT NULL column without a proven literal default is unsafe.");
            }
            Column existingColumn =
                    currentSchema == null
                            ? null
                            : currentSchema.getColumn(column.getName()).orElse(null);
            CatalogColumn existingCatalogColumn =
                    currentSchema == null
                            ? findCatalogColumn(catalogColumns, column.getName())
                            : null;
            if (existingColumn != null || existingCatalogColumn != null) {
                boolean identical =
                        existingColumn == null
                                ? existingCatalogColumn.matches(column)
                                : existingColumn.equals(column);
                if (!identical) {
                    throw new SchemaEvolveException(
                            event,
                            "Conflicting DuckLake column already exists: "
                                    + column.getName()
                                    + ".");
                }
                continue;
            }
            statements.add(
                    "ALTER TABLE "
                            + tableName(event.tableId())
                            + " ADD COLUMN "
                            + columnDefinition(column));
            unappliedColumns.add(addedColumn);
        }
        Schema schemaAfterCommit = currentSchema;
        if (currentSchema != null && !unappliedColumns.isEmpty()) {
            schemaAfterCommit =
                    SchemaUtils.applySchemaChangeEvent(
                            currentSchema, new AddColumnEvent(event.tableId(), unappliedColumns));
        }
        return new DuckLakeSchemaChangePlan(statements, schemaAfterCommit, null);
    }

    private DuckLakeSchemaChangePlan planDropColumns(DropColumnEvent event) {
        Schema currentSchema = repository.currentSchema(event.tableId());
        List<String> primaryKeys =
                currentSchema == null
                        ? repository.loadPrimaryKeys(event)
                        : currentSchema.primaryKeys();
        List<String> columnNames =
                currentSchema == null
                        ? repository.loadColumnNames(event)
                        : currentSchema.getColumnNames();
        Set<String> droppedColumns = new HashSet<>(repository.droppedColumns(event));
        List<String> statements = new ArrayList<>();
        List<String> unappliedColumns = new ArrayList<>();
        for (String columnName : event.getDroppedColumnNames()) {
            if (primaryKeys.contains(columnName)) {
                throw new UnsupportedSchemaChangeEventException(
                        event, "Dropping a primary-key column is not supported.");
            }
            if (!columnNames.contains(columnName)) {
                if (droppedColumns.contains(columnName)) {
                    continue;
                }
                throw new SchemaEvolveException(
                        event, "Cannot drop missing column " + columnName + ".");
            }
            statements.add(
                    "ALTER TABLE "
                            + tableName(event.tableId())
                            + " DROP COLUMN "
                            + DuckDbSqlUtils.quoteIdentifier(columnName));
            statements.add(
                    "INSERT INTO "
                            + droppedColumnTable()
                            + " (schema_name, table_name, column_name) VALUES ("
                            + DuckDbSqlUtils.literal(schemaName(event.tableId()))
                            + ", "
                            + DuckDbSqlUtils.literal(event.tableId().getTableName())
                            + ", "
                            + DuckDbSqlUtils.literal(columnName)
                            + ")");
            droppedColumns.add(columnName);
            unappliedColumns.add(columnName);
        }
        Schema schemaAfterCommit = currentSchema;
        if (currentSchema != null && !unappliedColumns.isEmpty()) {
            schemaAfterCommit =
                    SchemaUtils.applySchemaChangeEvent(
                            currentSchema, new DropColumnEvent(event.tableId(), unappliedColumns));
        }
        return new DuckLakeSchemaChangePlan(statements, schemaAfterCommit, droppedColumns);
    }

    private DuckLakeSchemaChangePlan planAlterColumnTypes(AlterColumnTypeEvent event) {
        Schema currentSchema = repository.currentSchema(event.tableId());
        List<String> primaryKeys =
                currentSchema == null
                        ? repository.loadPrimaryKeys(event)
                        : currentSchema.primaryKeys();
        Map<String, CatalogColumn> catalogColumns =
                currentSchema == null
                        ? repository.loadCatalogColumnMap(event)
                        : Collections.emptyMap();
        if (!event.getComments().isEmpty()) {
            throw new UnsupportedSchemaChangeEventException(
                    event, "Changing column comments together with a type is not supported.");
        }
        List<String> statements = new ArrayList<>();
        Map<String, DataType> unappliedOldTypes = new LinkedHashMap<>();
        Map<String, DataType> unappliedNewTypes = new LinkedHashMap<>();
        for (Map.Entry<String, DataType> typeChange : event.getTypeMapping().entrySet()) {
            String columnName = typeChange.getKey();
            DataType oldType = event.getOldTypeMapping().get(columnName);
            DataType newType = typeChange.getValue();
            DataType currentType =
                    currentSchema == null
                            ? null
                            : currentSchema.getColumn(columnName).map(Column::getType).orElse(null);
            CatalogColumn catalogColumn = catalogColumns.get(columnName);
            if (primaryKeys.contains(columnName)) {
                throw new UnsupportedSchemaChangeEventException(
                        event, "Changing a primary-key column is not supported.");
            }
            if (oldType == null) {
                throw new SchemaEvolveException(
                        event, "Missing or stale pre-schema type for column " + columnName + ".");
            }
            if (!DuckLakeTypeUtils.isSupportedDdlPromotion(oldType, newType)) {
                throw new UnsupportedSchemaChangeEventException(
                        event,
                        "DuckLake does not support the requested lossless type promotion: "
                                + oldType
                                + " -> "
                                + newType);
            }
            if (currentType == null && catalogColumn == null) {
                throw new SchemaEvolveException(
                        event, "Missing or stale pre-schema type for column " + columnName + ".");
            }
            String actualType =
                    currentSchema == null ? catalogColumn.type() : normalizedType(currentType);
            boolean actualNullable =
                    currentSchema == null ? catalogColumn.nullable() : currentType.isNullable();
            boolean typeAlreadyApplied = actualType.equals(normalizedType(newType));
            boolean nullabilityAlreadyApplied = actualNullable == newType.isNullable();
            if (typeAlreadyApplied && nullabilityAlreadyApplied) {
                continue;
            }
            if (!typeAlreadyApplied && !actualType.equals(normalizedType(oldType))) {
                throw new SchemaEvolveException(
                        event, "Missing or stale pre-schema type for column " + columnName + ".");
            }
            if (!typeAlreadyApplied) {
                statements.add(
                        "ALTER TABLE "
                                + tableName(event.tableId())
                                + " ALTER COLUMN "
                                + DuckDbSqlUtils.quoteIdentifier(columnName)
                                + " SET TYPE "
                                + DuckLakeTypeUtils.toDuckDbType(newType));
            }
            if (!nullabilityAlreadyApplied) {
                statements.add(nullabilityStatement(event.tableId(), columnName, newType));
            }
            unappliedOldTypes.put(columnName, oldType);
            unappliedNewTypes.put(columnName, newType);
        }
        Schema schemaAfterCommit = currentSchema;
        if (currentSchema != null && !unappliedNewTypes.isEmpty()) {
            schemaAfterCommit =
                    SchemaUtils.applySchemaChangeEvent(
                            currentSchema,
                            new AlterColumnTypeEvent(
                                    event.tableId(), unappliedNewTypes, unappliedOldTypes));
        }
        return new DuckLakeSchemaChangePlan(statements, schemaAfterCommit, null);
    }

    private void validateExistingTable(CreateTableEvent event, Schema expected) {
        List<CatalogColumn> actual = repository.loadCatalogColumns(event);
        if (actual.size() != expected.getColumns().size()) {
            throw conflictingCreate(event);
        }
        for (int i = 0; i < actual.size(); i++) {
            CatalogColumn actualColumn = actual.get(i);
            Column expectedColumn = expected.getColumns().get(i);
            if (!actualColumn.name().equals(expectedColumn.getName())
                    || !actualColumn.type().equals(normalizedType(expectedColumn.getType()))
                    || actualColumn.nullable() != expectedColumn.getType().isNullable()) {
                throw conflictingCreate(event);
            }
        }
        if (!repository.loadPrimaryKeys(event).equals(expected.primaryKeys())) {
            throw conflictingCreate(event);
        }
    }

    private static SchemaEvolveException conflictingCreate(CreateTableEvent event) {
        return new SchemaEvolveException(
                event, "Conflicting DuckLake table already exists: " + event.tableId());
    }

    private static void validateCreateTable(CreateTableEvent event, Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            throw new SchemaEvolveException(event, "DuckLake CDC tables require a primary key.");
        }
        for (String primaryKey : schema.primaryKeys()) {
            Column column =
                    schema.getColumn(primaryKey)
                            .orElseThrow(
                                    () ->
                                            new SchemaEvolveException(
                                                    event,
                                                    "Primary-key column does not exist: "
                                                            + primaryKey));
            if (column.getType().isNullable()) {
                throw new SchemaEvolveException(
                        event, "Primary-key column must be NOT NULL: " + primaryKey);
            }
        }
    }

    private static void validateColumn(SchemaChangeEvent event, Column column) {
        if (!column.isPhysical()) {
            throw new UnsupportedSchemaChangeEventException(
                    event, "DuckLake sink supports only physical columns.");
        }
        if (column.getDefaultValueExpression() != null) {
            throw new UnsupportedSchemaChangeEventException(
                    event,
                    "Column defaults require typed literal validation and are not supported yet.");
        }
        try {
            DuckLakeTypeUtils.toDuckDbType(column.getType());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedSchemaChangeEventException(event, e.getMessage(), e);
        }
    }

    private static CatalogColumn findCatalogColumn(List<CatalogColumn> columns, String columnName) {
        for (CatalogColumn column : columns) {
            if (column.name().equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    private static String normalizedType(DataType type) {
        return DuckLakeTypeUtils.normalizeDuckDbType(DuckLakeTypeUtils.toDuckDbType(type));
    }

    private static String nullabilityStatement(
            TableId tableId, String columnName, DataType newType) {
        return "ALTER TABLE "
                + tableName(tableId)
                + " ALTER COLUMN "
                + DuckDbSqlUtils.quoteIdentifier(columnName)
                + (newType.isNullable() ? " DROP NOT NULL" : " SET NOT NULL");
    }

    private static String columnDefinition(Column column) {
        return DuckDbSqlUtils.quoteIdentifier(column.getName())
                + " "
                + DuckLakeTypeUtils.toDuckDbType(column.getType())
                + (column.getType().isNullable() ? "" : " NOT NULL");
    }

    private static String tableName(TableId tableId) {
        return DuckDbSqlUtils.qualifiedName(
                DuckLakeCatalogObjects.CATALOG_ALIAS, schemaName(tableId), tableId.getTableName());
    }

    private static String primaryKeyTable() {
        return DuckDbSqlUtils.qualifiedName(
                DuckLakeCatalogObjects.CATALOG_ALIAS,
                DuckLakeCatalogObjects.INTERNAL_SCHEMA,
                DuckLakeCatalogObjects.PRIMARY_KEY_TABLE);
    }

    private static String droppedColumnTable() {
        return DuckDbSqlUtils.qualifiedName(
                DuckLakeCatalogObjects.CATALOG_ALIAS,
                DuckLakeCatalogObjects.INTERNAL_SCHEMA,
                DuckLakeCatalogObjects.DROPPED_COLUMN_TABLE);
    }

    private static String schemaName(TableId tableId) {
        return tableId.getSchemaName() == null
                ? DuckLakeCatalogObjects.DEFAULT_SCHEMA
                : tableId.getSchemaName();
    }
}
