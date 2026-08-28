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

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogOperations;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Applies one schema change inside the caller-owned DuckLake transaction. */
final class DuckLakeSchemaChangeExecutor {

    private final DuckLakeCatalogOperations client;
    private final Map<TableId, Schema> schemas = new HashMap<>();
    private final Map<TableId, Set<String>> droppedColumnNames = new HashMap<>();
    private final DuckLakeSchemaRepository repository;
    private final DuckLakeSchemaChangePlanner planner;

    DuckLakeSchemaChangeExecutor(DuckLakeCatalogOperations client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.repository =
                new DuckLakeSchemaRepository(() -> this.client, schemas, droppedColumnNames);
        this.planner = new DuckLakeSchemaChangePlanner(repository);
    }

    void apply(SchemaChangeEvent event) throws SQLException {
        DuckLakeSchemaChangePlan plan = planner.plan(event);
        for (String statement : plan.statements()) {
            client.execute(statement);
        }
        repository.update(event.tableId(), plan);
    }

    void clearCache() {
        schemas.clear();
        droppedColumnNames.clear();
    }
}
