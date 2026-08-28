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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeTypeUtils;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Validates schema changes that are applied atomically by the checkpoint committer. */
@Internal
public final class DuckLakeMetadataApplier implements MetadataApplier {

    private static final long serialVersionUID = 1L;
    private static final Set<SchemaChangeEventType> SUPPORTED_EVENT_TYPES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            SchemaChangeEventType.CREATE_TABLE,
                            SchemaChangeEventType.ADD_COLUMN,
                            SchemaChangeEventType.DROP_COLUMN,
                            SchemaChangeEventType.RENAME_COLUMN,
                            SchemaChangeEventType.ALTER_COLUMN_TYPE));

    private Set<SchemaChangeEventType> acceptedEventTypes = SUPPORTED_EVENT_TYPES;

    public DuckLakeMetadataApplier() {}

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws SchemaEvolveException {
        validateSchemaChangeEvent(event);
        validateSchemaChange(event);
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

    private static void validateSchemaChange(SchemaChangeEvent event) {
        SchemaChangeEventVisitor.voidVisit(
                event,
                DuckLakeMetadataApplier::validateAddColumns,
                DuckLakeMetadataApplier::validateAlterColumnTypes,
                DuckLakeMetadataApplier::validateCreateTable,
                ignored -> {},
                unsupported -> {
                    throw new UnsupportedSchemaChangeEventException(unsupported);
                },
                DuckLakeMetadataApplier::validateRenameColumns,
                unsupported -> {
                    throw new UnsupportedSchemaChangeEventException(unsupported);
                },
                unsupported -> {
                    throw new UnsupportedSchemaChangeEventException(unsupported);
                });
    }

    private static void validateCreateTable(CreateTableEvent event) {
        Schema schema = event.getSchema();
        if (schema.primaryKeys().isEmpty()) {
            throw new SchemaEvolveException(event, "DuckLake CDC tables require a primary key.");
        }
        for (Column column : schema.getColumns()) {
            validateColumn(event, column);
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

    private static void validateAddColumns(AddColumnEvent event) {
        for (AddColumnEvent.ColumnWithPosition addedColumn : event.getAddedColumns()) {
            if (addedColumn.getPosition() != AddColumnEvent.ColumnPosition.LAST) {
                throw new UnsupportedSchemaChangeEventException(
                        event, "DuckLake cannot preserve non-LAST column positions.");
            }
            Column column = addedColumn.getAddColumn();
            validateColumn(event, column);
            if (!column.getType().isNullable()) {
                throw new UnsupportedSchemaChangeEventException(
                        event,
                        "Adding a NOT NULL column without a proven literal default is unsafe.");
            }
        }
    }

    private static void validateAlterColumnTypes(AlterColumnTypeEvent event) {
        if (!event.getComments().isEmpty()) {
            throw new UnsupportedSchemaChangeEventException(
                    event, "Changing column comments together with a type is not supported.");
        }
        for (Map.Entry<String, org.apache.flink.cdc.common.types.DataType> change :
                event.getTypeMapping().entrySet()) {
            org.apache.flink.cdc.common.types.DataType oldType =
                    event.getOldTypeMapping().get(change.getKey());
            if (oldType == null) {
                throw new SchemaEvolveException(
                        event,
                        "Missing or stale pre-schema type for column " + change.getKey() + ".");
            }
            if (!DuckLakeTypeUtils.isSupportedDdlPromotion(oldType, change.getValue())) {
                throw new UnsupportedSchemaChangeEventException(
                        event,
                        "DuckLake does not support the requested lossless type promotion: "
                                + oldType
                                + " -> "
                                + change.getValue());
            }
        }
    }

    private static void validateRenameColumns(RenameColumnEvent event) {
        if (event.getNameMapping().isEmpty()) {
            throw new SchemaEvolveException(event, "Rename column mapping must not be empty.");
        }
        if (event.getNameMapping().values().stream().distinct().count()
                != event.getNameMapping().size()) {
            throw new SchemaEvolveException(event, "Rename column targets must be unique.");
        }
        Set<String> sources = new HashSet<>(event.getNameMapping().keySet());
        if (event.getNameMapping().values().stream().anyMatch(sources::contains)) {
            throw new UnsupportedSchemaChangeEventException(
                    event, "Chained or cyclic column renames are not supported.");
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
}
