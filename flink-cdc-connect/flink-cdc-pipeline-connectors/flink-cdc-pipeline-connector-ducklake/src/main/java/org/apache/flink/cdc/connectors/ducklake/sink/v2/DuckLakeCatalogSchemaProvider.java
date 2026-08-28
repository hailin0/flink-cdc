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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckDbConnectionFactory;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogClient;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeTypeUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/** Loads schemas for writers created without local state during savepoint rescaling. */
@Internal
final class DuckLakeCatalogSchemaProvider implements DuckLakeWriter.SchemaProvider {

    private static final String CATALOG = DuckLakeCatalogObjects.CATALOG_ALIAS;
    private static final String INTERNAL_SCHEMA = DuckLakeCatalogObjects.INTERNAL_SCHEMA;
    private static final String PRIMARY_KEY_TABLE = DuckLakeCatalogObjects.PRIMARY_KEY_TABLE;

    private final DuckDbConnectionFactory connectionFactory;
    private DuckLakeCatalogClient client;

    DuckLakeCatalogSchemaProvider(DuckDbConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public Schema load(TableId tableId) throws IOException {
        if (DuckLakeCatalogObjects.isInternalSchema(tableId)) {
            throw new IOException("Target schema " + INTERNAL_SCHEMA + " is reserved");
        }
        String schemaName =
                tableId.getSchemaName() == null
                        ? DuckLakeCatalogObjects.DEFAULT_SCHEMA
                        : tableId.getSchemaName();
        try {
            List<CatalogColumn> columns =
                    client().query(
                                    "DESCRIBE SELECT * FROM "
                                            + DuckDbSqlUtils.qualifiedName(
                                                    CATALOG, schemaName, tableId.getTableName()),
                                    resultSet ->
                                            new CatalogColumn(
                                                    resultSet.getString(1),
                                                    resultSet.getString(2),
                                                    "YES"
                                                            .equalsIgnoreCase(
                                                                    resultSet.getString(3))));
            List<String> primaryKeys =
                    client().query(
                                    "SELECT column_name FROM "
                                            + DuckDbSqlUtils.qualifiedName(
                                                    CATALOG, INTERNAL_SCHEMA, PRIMARY_KEY_TABLE)
                                            + " WHERE schema_name = "
                                            + DuckDbSqlUtils.literal(schemaName)
                                            + " AND table_name = "
                                            + DuckDbSqlUtils.literal(tableId.getTableName())
                                            + " ORDER BY key_order",
                                    resultSet -> resultSet.getString(1));
            if (columns.isEmpty() || primaryKeys.isEmpty()) {
                throw new IOException(
                        "DuckLake schema or primary-key metadata is missing for " + tableId);
            }
            Schema.Builder builder = Schema.newBuilder();
            for (CatalogColumn column : columns) {
                DataType type = DuckLakeTypeUtils.fromDuckDbType(column.type, column.nullable);
                builder.physicalColumn(column.name, type);
            }
            return builder.primaryKey(primaryKeys).build();
        } catch (SQLException | IllegalArgumentException e) {
            throw new IOException("Failed to load DuckLake schema for " + tableId, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            try {
                client.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close DuckLake schema provider", e);
            } finally {
                client = null;
            }
        }
    }

    private DuckLakeCatalogClient client() throws SQLException {
        if (client == null) {
            client = new DuckLakeCatalogClient(connectionFactory);
        }
        return client;
    }

    private static final class CatalogColumn {
        private final String name;
        private final String type;
        private final boolean nullable;

        private CatalogColumn(String name, String type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
        }
    }
}
