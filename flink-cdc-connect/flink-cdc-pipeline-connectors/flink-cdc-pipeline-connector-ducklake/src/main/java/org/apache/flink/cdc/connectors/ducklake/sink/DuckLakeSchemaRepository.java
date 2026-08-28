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

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogOperations;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeTypeUtils;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reads DuckLake schemas and keeps the metadata applier's local schema cache. */
final class DuckLakeSchemaRepository {

    private final CatalogClientProvider clientProvider;
    private final Map<TableId, Schema> schemas;
    private final Map<TableId, Set<String>> droppedColumnNames;

    DuckLakeSchemaRepository(
            CatalogClientProvider clientProvider,
            Map<TableId, Schema> schemas,
            Map<TableId, Set<String>> droppedColumnNames) {
        this.clientProvider = clientProvider;
        this.schemas = schemas;
        this.droppedColumnNames = droppedColumnNames;
    }

    @Nullable
    Schema currentSchema(TableId tableId) {
        return schemas.get(tableId);
    }

    void invalidate(TableId tableId) {
        schemas.remove(tableId);
        droppedColumnNames.remove(tableId);
    }

    void update(TableId tableId, DuckLakeSchemaChangePlan change) {
        if (change.schemaAfterCommit() != null) {
            schemas.put(tableId, change.schemaAfterCommit());
        }
        if (change.droppedColumnsAfterCommit() != null) {
            droppedColumnNames.put(tableId, new HashSet<>(change.droppedColumnsAfterCommit()));
        }
    }

    boolean tableExists(SchemaChangeEvent event) {
        try {
            return !clientProvider
                    .get()
                    .query(
                            "SELECT table_name FROM information_schema.tables"
                                    + " WHERE table_catalog = "
                                    + DuckDbSqlUtils.literal(DuckLakeCatalogObjects.CATALOG_ALIAS)
                                    + " AND table_schema = "
                                    + DuckDbSqlUtils.literal(schemaName(event.tableId()))
                                    + " AND table_name = "
                                    + DuckDbSqlUtils.literal(event.tableId().getTableName()),
                            resultSet -> resultSet.getString(1))
                    .isEmpty();
        } catch (SQLException e) {
            throw new SchemaEvolveException(event, "Unable to inspect DuckLake table", e);
        }
    }

    List<String> loadPrimaryKeys(SchemaChangeEvent event) {
        try {
            return clientProvider
                    .get()
                    .query(
                            "SELECT column_name FROM "
                                    + primaryKeyTable()
                                    + " WHERE schema_name = "
                                    + DuckDbSqlUtils.literal(schemaName(event.tableId()))
                                    + " AND table_name = "
                                    + DuckDbSqlUtils.literal(event.tableId().getTableName())
                                    + " ORDER BY key_order",
                            resultSet -> resultSet.getString(1));
        } catch (SQLException e) {
            throw new SchemaEvolveException(
                    event, "Unable to load DuckLake primary-key metadata.", e);
        }
    }

    List<String> loadColumnNames(SchemaChangeEvent event) {
        List<String> names = new ArrayList<>();
        for (CatalogColumn column : loadCatalogColumns(event)) {
            names.add(column.name());
        }
        return names;
    }

    Map<String, CatalogColumn> loadCatalogColumnMap(SchemaChangeEvent event) {
        Map<String, CatalogColumn> columns = new HashMap<>();
        for (CatalogColumn column : loadCatalogColumns(event)) {
            columns.put(column.name(), column);
        }
        return columns;
    }

    List<CatalogColumn> loadCatalogColumns(SchemaChangeEvent event) {
        try {
            return clientProvider
                    .get()
                    .query(
                            "DESCRIBE SELECT * FROM " + tableName(event.tableId()),
                            resultSet ->
                                    new CatalogColumn(
                                            resultSet.getString(1),
                                            resultSet.getString(2),
                                            resultSet.getString(3)));
        } catch (SQLException e) {
            throw new SchemaEvolveException(event, "Unable to load DuckLake table schema.", e);
        }
    }

    Set<String> droppedColumns(SchemaChangeEvent event) {
        Set<String> cached = droppedColumnNames.get(event.tableId());
        if (cached != null) {
            return cached;
        }
        try {
            Set<String> loaded =
                    new HashSet<>(
                            clientProvider
                                    .get()
                                    .query(
                                            "SELECT column_name FROM "
                                                    + droppedColumnTable()
                                                    + " WHERE schema_name = "
                                                    + DuckDbSqlUtils.literal(
                                                            schemaName(event.tableId()))
                                                    + " AND table_name = "
                                                    + DuckDbSqlUtils.literal(
                                                            event.tableId().getTableName()),
                                            resultSet -> resultSet.getString(1)));
            droppedColumnNames.put(event.tableId(), loaded);
            return loaded;
        } catch (SQLException e) {
            throw new SchemaEvolveException(
                    event, "Unable to load dropped DuckLake column metadata.", e);
        }
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

    private static String tableName(TableId tableId) {
        return DuckDbSqlUtils.qualifiedName(
                DuckLakeCatalogObjects.CATALOG_ALIAS, schemaName(tableId), tableId.getTableName());
    }

    private static String schemaName(TableId tableId) {
        return tableId.getSchemaName() == null
                ? DuckLakeCatalogObjects.DEFAULT_SCHEMA
                : tableId.getSchemaName();
    }

    @FunctionalInterface
    interface CatalogClientProvider {
        DuckLakeCatalogOperations get() throws SQLException;
    }

    /** Column metadata returned by DuckDB's DESCRIBE statement. */
    static final class CatalogColumn {
        private final String name;
        private final String type;
        private final boolean nullable;

        private CatalogColumn(String name, String type, String nullable) {
            this.name = name;
            this.type = DuckLakeTypeUtils.normalizeDuckDbType(type);
            this.nullable = "YES".equalsIgnoreCase(nullable);
        }

        String name() {
            return name;
        }

        String type() {
            return type;
        }

        boolean nullable() {
            return nullable;
        }

        boolean matches(Column column) {
            return name.equals(column.getName())
                    && type.equals(normalizedType(column.getType()))
                    && nullable == column.getType().isNullable();
        }

        private static String normalizedType(DataType type) {
            return DuckLakeTypeUtils.normalizeDuckDbType(DuckLakeTypeUtils.toDuckDbType(type));
        }
    }
}
