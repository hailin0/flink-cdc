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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckDbConnectionFactory;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeCatalogClient;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckLakeTableCoordinator;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeRetryBackoff;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeSqlExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Applies the DDL subset that is safe with asynchronous checkpoint data commits. */
@Internal
public final class DuckLakeMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(DuckLakeMetadataApplier.class);

    private static final long serialVersionUID = 1L;
    private static final Set<SchemaChangeEventType> SUPPORTED_EVENT_TYPES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            SchemaChangeEventType.CREATE_TABLE,
                            SchemaChangeEventType.ADD_COLUMN,
                            SchemaChangeEventType.DROP_COLUMN,
                            SchemaChangeEventType.ALTER_COLUMN_TYPE));

    private final DuckLakeSinkConfig config;
    private final int maxRetries;
    private final long retryBackoffMillis;
    private final boolean tableCoordinationEnabled;
    private final Map<TableId, Schema> schemas = new HashMap<>();
    private final Map<TableId, Set<String>> droppedColumnNames = new HashMap<>();
    private Set<SchemaChangeEventType> acceptedEventTypes = SUPPORTED_EVENT_TYPES;
    private transient DuckLakeCatalogClient client;
    private transient DuckLakeTableCoordinator tableCoordinator;
    private transient DuckLakeSchemaRepository schemaRepository;
    private transient DuckLakeSchemaChangePlanner schemaChangePlanner;

    public DuckLakeMetadataApplier(DuckLakeSinkConfig config) {
        this.config = config;
        this.maxRetries = config.getCommitMaxRetries();
        this.retryBackoffMillis = config.getCommitRetryBackoff().toMillis();
        this.tableCoordinationEnabled = true;
    }

    @VisibleForTesting
    DuckLakeMetadataApplier(DuckLakeCatalogClient client) {
        this(client, 0, Duration.ZERO, false);
    }

    @VisibleForTesting
    DuckLakeMetadataApplier(DuckLakeCatalogClient client, int maxRetries, Duration retryBackoff) {
        this(client, maxRetries, retryBackoff, true);
    }

    @VisibleForTesting
    DuckLakeMetadataApplier(
            DuckLakeCatalogClient client,
            int maxRetries,
            Duration retryBackoff,
            boolean tableCoordinationEnabled) {
        this.config = null;
        this.client = client;
        this.maxRetries = maxRetries;
        this.retryBackoffMillis = retryBackoff.toMillis();
        this.tableCoordinationEnabled = tableCoordinationEnabled;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws SchemaEvolveException {
        validateSchemaChangeEvent(event);
        planner().plan(event);

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            DuckLakeCatalogClient catalogClient = null;
            boolean transactionStarted = false;
            try {
                catalogClient = client();
                if (tableCoordinationEnabled) {
                    tableCoordinator(catalogClient).prepareTable(event.tableId());
                }
                catalogClient.begin();
                transactionStarted = true;
                if (tableCoordinationEnabled) {
                    tableCoordinator(catalogClient).lockTable(event.tableId());
                    repository().invalidate(event.tableId());
                }
                DuckLakeSchemaChangePlan change = planner().plan(event);
                for (String sql : change.statements()) {
                    catalogClient.execute(sql);
                }
                catalogClient.commit();
                repository().update(event.tableId(), change);
                return;
            } catch (Exception failure) {
                if (transactionStarted && catalogClient != null) {
                    try {
                        catalogClient.rollback();
                    } catch (SQLException rollbackFailure) {
                        failure.addSuppressed(rollbackFailure);
                    }
                }
                if (failure instanceof SchemaEvolveException) {
                    throw (SchemaEvolveException) failure;
                }
                if (!(failure instanceof SQLException)) {
                    throw new SchemaEvolveException(
                            event, "Failed to apply DuckLake schema change", failure);
                }
                SQLException sqlFailure = (SQLException) failure;
                if (!DuckLakeSqlExceptionUtils.isRetryable(sqlFailure) || attempt == maxRetries) {
                    throw new SchemaEvolveException(
                            event, "Failed to apply DuckLake schema change", failure);
                }
                repository().invalidate(event.tableId());
                if (DuckLakeSqlExceptionUtils.isConnectionFailure(sqlFailure) && config != null) {
                    resetClient(sqlFailure);
                }
                LOG.warn(
                        "Retrying DuckLake schema change {} after failed attempt {} of {}",
                        event,
                        attempt + 1,
                        maxRetries + 1,
                        failure);
                sleepBeforeRetry(event, attempt);
            }
        }
        throw new IllegalStateException("Unreachable DuckLake schema retry state");
    }

    private void validateSchemaChangeEvent(SchemaChangeEvent event) {
        if (!acceptedEventTypes.contains(event.getType())) {
            throw new UnsupportedSchemaChangeEventException(event);
        }
        if (event.tableId().getNamespace() != null) {
            throw new UnsupportedSchemaChangeEventException(
                    event,
                    "Three-part table identifiers must be routed to a two-part target identifier.");
        }
        String reservedSchema = DuckLakeCatalogObjects.reservedSchemaName(event.tableId());
        if (reservedSchema != null) {
            throw new UnsupportedSchemaChangeEventException(
                    event,
                    "Target schema " + reservedSchema + " is reserved by the DuckLake connector.");
        }
    }

    private void resetClient(SQLException failure) {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (SQLException closeFailure) {
            failure.addSuppressed(closeFailure);
        } finally {
            client = null;
            tableCoordinator = null;
        }
    }

    private void sleepBeforeRetry(SchemaChangeEvent event, int attempt) {
        try {
            DuckLakeRetryBackoff.sleep(retryBackoffMillis, attempt, Thread::sleep);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new SchemaEvolveException(
                    event, "Interrupted while retrying DuckLake schema change", interrupted);
        }
    }

    private DuckLakeSchemaChangePlanner planner() {
        if (schemaChangePlanner == null) {
            schemaChangePlanner = new DuckLakeSchemaChangePlanner(repository());
        }
        return schemaChangePlanner;
    }

    private DuckLakeSchemaRepository repository() {
        if (schemaRepository == null) {
            schemaRepository =
                    new DuckLakeSchemaRepository(this::client, schemas, droppedColumnNames);
        }
        return schemaRepository;
    }

    private DuckLakeCatalogClient client() throws SQLException {
        if (client == null) {
            if (config == null) {
                throw new IllegalStateException("DuckLake catalog client is unavailable");
            }
            client = new DuckLakeCatalogClient(new DuckDbConnectionFactory(config));
        }
        return client;
    }

    private DuckLakeTableCoordinator tableCoordinator(DuckLakeCatalogClient catalogClient) {
        if (tableCoordinator == null) {
            tableCoordinator = new DuckLakeTableCoordinator(catalogClient);
        }
        return tableCoordinator;
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        EnumSet<SchemaChangeEventType> accepted = EnumSet.copyOf(SUPPORTED_EVENT_TYPES);
        accepted.retainAll(schemaEvolutionTypes);
        acceptedEventTypes = Collections.unmodifiableSet(accepted);
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return acceptedEventTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return SUPPORTED_EVENT_TYPES;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            try {
                client.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close DuckLake catalog client", e);
            } finally {
                client = null;
                tableCoordinator = null;
            }
        }
    }
}
