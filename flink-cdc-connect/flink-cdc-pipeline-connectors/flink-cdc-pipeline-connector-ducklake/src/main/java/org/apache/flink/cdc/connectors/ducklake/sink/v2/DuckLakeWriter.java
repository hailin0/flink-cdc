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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Stateful sink writer that turns CDC table batches into immutable Parquet file pairs. */
@Internal
public final class DuckLakeWriter
        implements CommittingSinkWriter<Event, DuckLakeCommittable>,
                StatefulSinkWriter<Event, DuckLakeWriterState> {

    private final ConnectionProvider connectionProvider;
    private final SchemaProvider schemaProvider;
    private final DuckLakeFileLayout fileLayout;
    private final int subtaskId;
    private final int attemptNumber;
    private final ZoneId pipelineZone;
    private final String sinkId;
    private final String operatorId;
    private final long maxEventsPerFile;
    private final long maxBufferedEvents;
    private final int maxOpenTables;

    private final Map<TableId, Schema> currentSchemas;
    private final Map<TableId, Integer> schemaBatchIndexes;
    private final Map<TableId, Long> fileSequences;
    private final Map<TableId, DuckLakeTableBuffer> tableBuffers;
    private final List<DuckLakeWriteResult> pendingWriteResults;
    private final List<DuckLakeSchemaChangeResult> pendingSchemaChangeResults;

    private long writerEpoch;
    private long nextBufferId;
    private long bufferedEventCount;
    private Connection writerConnection;
    private boolean closed;

    public DuckLakeWriter(
            ConnectionProvider connectionProvider,
            String storageRoot,
            int subtaskId,
            int attemptNumber,
            ZoneId pipelineZone,
            long writerEpoch,
            String sinkId,
            String operatorId,
            @Nullable DuckLakeWriterState restoredState) {
        this(
                connectionProvider,
                tableId -> {
                    throw new IOException("No DuckLake schema provider configured for " + tableId);
                },
                storageRoot,
                subtaskId,
                attemptNumber,
                pipelineZone,
                writerEpoch,
                sinkId,
                operatorId,
                restoredState,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Integer.MAX_VALUE);
    }

    public DuckLakeWriter(
            ConnectionProvider connectionProvider,
            SchemaProvider schemaProvider,
            String storageRoot,
            int subtaskId,
            int attemptNumber,
            ZoneId pipelineZone,
            long writerEpoch,
            String sinkId,
            String operatorId,
            @Nullable DuckLakeWriterState restoredState) {
        this(
                connectionProvider,
                schemaProvider,
                storageRoot,
                subtaskId,
                attemptNumber,
                pipelineZone,
                writerEpoch,
                sinkId,
                operatorId,
                restoredState,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Integer.MAX_VALUE);
    }

    DuckLakeWriter(
            ConnectionProvider connectionProvider,
            SchemaProvider schemaProvider,
            String storageRoot,
            int subtaskId,
            int attemptNumber,
            ZoneId pipelineZone,
            long writerEpoch,
            String sinkId,
            String operatorId,
            @Nullable DuckLakeWriterState restoredState,
            long maxEventsPerFile) {
        this(
                connectionProvider,
                schemaProvider,
                storageRoot,
                subtaskId,
                attemptNumber,
                pipelineZone,
                writerEpoch,
                sinkId,
                operatorId,
                restoredState,
                maxEventsPerFile,
                Long.MAX_VALUE,
                Integer.MAX_VALUE);
    }

    DuckLakeWriter(
            ConnectionProvider connectionProvider,
            SchemaProvider schemaProvider,
            String storageRoot,
            int subtaskId,
            int attemptNumber,
            ZoneId pipelineZone,
            long writerEpoch,
            String sinkId,
            String operatorId,
            @Nullable DuckLakeWriterState restoredState,
            long maxEventsPerFile,
            long maxBufferedEvents,
            int maxOpenTables) {
        this.connectionProvider =
                Objects.requireNonNull(connectionProvider, "connectionProvider must not be null");
        this.schemaProvider =
                Objects.requireNonNull(schemaProvider, "schemaProvider must not be null");
        if (subtaskId < 0 || attemptNumber < 0 || writerEpoch < 0) {
            throw new IllegalArgumentException(
                    "subtaskId, attemptNumber, and writerEpoch must be non-negative");
        }
        this.subtaskId = subtaskId;
        this.attemptNumber = attemptNumber;
        this.pipelineZone = Objects.requireNonNull(pipelineZone, "pipelineZone must not be null");
        this.writerEpoch = writerEpoch;
        this.sinkId = Objects.requireNonNull(sinkId, "sinkId must not be null");
        this.operatorId = Objects.requireNonNull(operatorId, "operatorId must not be null");
        this.fileLayout =
                new DuckLakeFileLayout(
                        storageRoot, subtaskId, attemptNumber, this.sinkId, this.operatorId);
        if (maxEventsPerFile <= 0) {
            throw new IllegalArgumentException("maxEventsPerFile must be positive");
        }
        if (maxBufferedEvents <= 0) {
            throw new IllegalArgumentException("maxBufferedEvents must be positive");
        }
        if (maxOpenTables <= 0) {
            throw new IllegalArgumentException("maxOpenTables must be positive");
        }
        this.maxEventsPerFile = maxEventsPerFile;
        this.maxBufferedEvents = maxBufferedEvents;
        this.maxOpenTables = maxOpenTables;
        this.tableBuffers = new LinkedHashMap<>(16, 0.75f, true);

        if (restoredState == null) {
            this.currentSchemas = new HashMap<>();
            this.schemaBatchIndexes = new HashMap<>();
            this.fileSequences = new HashMap<>();
            this.pendingWriteResults = new ArrayList<>();
            this.pendingSchemaChangeResults = new ArrayList<>();
        } else {
            validateRestoredState(restoredState);
            this.currentSchemas = new HashMap<>(restoredState.getCurrentSchemas());
            this.schemaBatchIndexes = new HashMap<>(restoredState.getSchemaBatchIndexes());
            this.fileSequences = new HashMap<>(restoredState.getFileSequences());
            this.pendingWriteResults = new ArrayList<>(restoredState.getPendingWriteResults());
            this.pendingSchemaChangeResults =
                    new ArrayList<>(restoredState.getPendingSchemaChangeResults());
        }
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        ensureOpen();
        if (event instanceof DataChangeEvent) {
            writeDataChangeEvent((DataChangeEvent) event);
            return;
        }
        if (event instanceof SchemaChangeEvent) {
            writeSchemaChangeEvent((SchemaChangeEvent) event);
            return;
        }
        throw new IllegalArgumentException("Unsupported DuckLake sink event: " + event.getClass());
    }

    @Override
    public Collection<DuckLakeCommittable> prepareCommit() throws IOException {
        ensureOpen();
        flushAllTableBuffers();
        List<DuckLakeCommittable> result =
                new ArrayList<>(pendingWriteResults.size() + pendingSchemaChangeResults.size());
        for (DuckLakeWriteResult writeResult : pendingWriteResults) {
            result.add(
                    new DuckLakeCommittable(
                            sinkId,
                            operatorId,
                            writeResult,
                            DuckLakeCommittableSerializer.hashWriteResult(writeResult)));
        }
        for (DuckLakeSchemaChangeResult schemaChangeResult : pendingSchemaChangeResults) {
            result.add(
                    new DuckLakeCommittable(
                            sinkId,
                            operatorId,
                            schemaChangeResult,
                            DuckLakeCommittableSerializer.hashSchemaChangeResult(
                                    schemaChangeResult)));
        }
        pendingWriteResults.clear();
        pendingSchemaChangeResults.clear();
        schemaBatchIndexes.clear();
        fileSequences.clear();
        writerEpoch++;
        return result;
    }

    @Override
    public List<DuckLakeWriterState> snapshotState(long checkpointId) {
        ensureOpen();
        return Collections.singletonList(
                new DuckLakeWriterState(
                        sinkId,
                        operatorId,
                        writerEpoch,
                        currentSchemas,
                        schemaBatchIndexes,
                        fileSequences,
                        pendingWriteResults,
                        pendingSchemaChangeResults));
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        ensureOpen();
        flushAllTableBuffers();
    }

    @Override
    public void writeWatermark(Watermark watermark) {}

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        for (DuckLakeTableBuffer buffer : tableBuffers.values()) {
            try {
                buffer.close();
            } catch (SQLException e) {
                if (failure == null) {
                    failure = new IOException("Failed to close DuckLake writer buffer", e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        tableBuffers.clear();
        bufferedEventCount = 0;
        try {
            schemaProvider.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (writerConnection != null) {
            try {
                writerConnection.close();
            } catch (SQLException e) {
                if (failure == null) {
                    failure = new IOException("Failed to close DuckDB writer connection", e);
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                writerConnection = null;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void writeDataChangeEvent(DataChangeEvent event) throws IOException {
        validateTargetTable(event.tableId());
        Schema schema = currentSchemas.get(event.tableId());
        if (schema == null) {
            schema = schemaProvider.load(event.tableId());
            DuckLakeTableBuffer.validateSchema(schema);
            currentSchemas.put(event.tableId(), schema);
            schemaBatchIndexes.putIfAbsent(event.tableId(), 0);
            fileSequences.putIfAbsent(event.tableId(), 0L);
        }
        DuckLakeTableBuffer buffer = tableBuffers.get(event.tableId());
        try {
            if (buffer == null) {
                flushLeastRecentlyUsedTableIfNecessary();
                buffer =
                        new DuckLakeTableBuffer(
                                writerConnection(),
                                event.tableId(),
                                schema,
                                pipelineZone,
                                Long.toString(nextBufferId++),
                                false);
                tableBuffers.put(event.tableId(), buffer);
            }
            buffer.append(event);
            bufferedEventCount++;
            if (buffer.getBufferedEventCount() >= maxEventsPerFile) {
                flushTableBuffer(event.tableId());
            } else if (bufferedEventCount >= maxBufferedEvents) {
                flushLeastRecentlyUsedTable();
            }
        } catch (SQLException e) {
            throw new IOException("Failed to buffer DuckLake CDC event for " + event.tableId(), e);
        }
    }

    private void writeSchemaChangeEvent(SchemaChangeEvent event) throws IOException {
        validateTargetTable(event.tableId());
        validateSupportedSchemaEvent(event);
        TableId tableId = event.tableId();
        if (event instanceof CreateTableEvent) {
            Schema schema = ((CreateTableEvent) event).getSchema();
            Schema currentSchema = currentSchemas.get(tableId);
            if (currentSchema != null) {
                if (currentSchema.equals(schema)) {
                    return;
                }
                throw new IllegalArgumentException("Conflicting create table event for " + tableId);
            }
            DuckLakeTableBuffer.validateSchema(schema);
            currentSchemas.put(tableId, schema);
            schemaBatchIndexes.putIfAbsent(tableId, 0);
            fileSequences.putIfAbsent(tableId, 0L);
            pendingSchemaChangeResults.add(
                    new DuckLakeSchemaChangeResult(
                            writerEpoch, 0, subtaskId, attemptNumber, event));
            return;
        }

        Schema oldSchema = currentSchemas.get(tableId);
        if (oldSchema == null) {
            oldSchema = schemaProvider.load(tableId);
            DuckLakeTableBuffer.validateSchema(oldSchema);
            currentSchemas.put(tableId, oldSchema);
            schemaBatchIndexes.put(tableId, 0);
            fileSequences.put(tableId, 0L);
        }
        flushTableBuffer(tableId);
        int nextBatch = schemaBatchIndexes.getOrDefault(tableId, 0) + 1;
        if (event instanceof RenameColumnEvent) {
            RenameColumnEvent rename = (RenameColumnEvent) event;
            for (String primaryKey : oldSchema.primaryKeys()) {
                if (rename.getNameMapping().containsKey(primaryKey)) {
                    throw new IllegalArgumentException(
                            "Renaming a primary-key column is not supported: " + primaryKey);
                }
            }
        }
        Schema newSchema = SchemaUtils.applySchemaChangeEvent(oldSchema, event);
        DuckLakeTableBuffer.validateSchema(newSchema);
        if (!oldSchema.primaryKeys().equals(newSchema.primaryKeys())) {
            throw new IllegalArgumentException(
                    "Changing primary keys is not supported for " + tableId);
        }
        schemaBatchIndexes.put(tableId, nextBatch);
        fileSequences.put(tableId, 0L);
        pendingSchemaChangeResults.add(
                new DuckLakeSchemaChangeResult(
                        writerEpoch, nextBatch, subtaskId, attemptNumber, event));
        currentSchemas.put(tableId, newSchema);
    }

    private void flushAllTableBuffers() throws IOException {
        List<TableId> tableIds = new ArrayList<>(tableBuffers.keySet());
        tableIds.sort(Comparator.comparing(TableId::identifier));
        for (TableId tableId : tableIds) {
            flushTableBuffer(tableId);
        }
    }

    private void flushLeastRecentlyUsedTableIfNecessary() throws IOException {
        if (tableBuffers.size() >= maxOpenTables) {
            flushLeastRecentlyUsedTable();
        }
    }

    private void flushLeastRecentlyUsedTable() throws IOException {
        if (tableBuffers.isEmpty()) {
            return;
        }
        flushTableBuffer(tableBuffers.keySet().iterator().next());
    }

    private void flushTableBuffer(TableId tableId) throws IOException {
        DuckLakeTableBuffer buffer = tableBuffers.get(tableId);
        if (buffer == null) {
            return;
        }
        int batchIndex = schemaBatchIndexes.getOrDefault(tableId, 0);
        long fileSequence = fileSequences.getOrDefault(tableId, 0L);
        String materializationId = UUID.randomUUID().toString();
        String dataPath =
                fileLayout.dataFile(
                        tableId, writerEpoch, batchIndex, fileSequence, materializationId);
        String keyPath =
                fileLayout.keyFile(
                        tableId, writerEpoch, batchIndex, fileSequence, materializationId);
        fileLayout.createParentDirectories(dataPath);
        fileLayout.createParentDirectories(keyPath);
        tableBuffers.remove(tableId);
        bufferedEventCount -= buffer.getBufferedEventCount();
        try {
            DuckLakeWriteResult writeResult =
                    buffer.flushToFiles(
                            writerEpoch,
                            batchIndex,
                            subtaskId,
                            attemptNumber,
                            fileSequence,
                            dataPath,
                            keyPath);
            pendingWriteResults.add(writeResult);
            fileSequences.put(tableId, fileSequence + 1);
        } catch (SQLException e) {
            throw new IOException("Failed to write DuckLake Parquet files for " + tableId, e);
        }
    }

    private void validateRestoredState(DuckLakeWriterState restoredState) {
        if (!sinkId.equals(restoredState.getSinkId())
                || !operatorId.equals(restoredState.getOperatorId())) {
            throw new IllegalArgumentException("Restored DuckLake writer IDs do not match");
        }
        if (writerEpoch != restoredState.getWriterEpoch()) {
            throw new IllegalArgumentException("Restored DuckLake writer epoch does not match");
        }
    }

    private static void validateSupportedSchemaEvent(SchemaChangeEvent event) {
        if (!(event instanceof CreateTableEvent)
                && !(event instanceof AddColumnEvent)
                && !(event instanceof DropColumnEvent)
                && !(event instanceof RenameColumnEvent)
                && !(event instanceof AlterColumnTypeEvent)) {
            throw new IllegalArgumentException(
                    "Unsupported DuckLake schema event: " + event.getClass().getSimpleName());
        }
    }

    private static void validateTargetTable(TableId tableId) {
        if (tableId.getNamespace() != null) {
            throw new IllegalArgumentException(
                    "Three-part target table identifiers are not supported: " + tableId);
        }
        String reservedSchema = DuckLakeCatalogObjects.reservedSchemaName(tableId);
        if (reservedSchema != null) {
            throw new IllegalArgumentException(
                    "Target schema " + reservedSchema + " is reserved by the DuckLake connector");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("DuckLake writer is closed");
        }
    }

    private Connection writerConnection() throws SQLException {
        if (writerConnection == null) {
            writerConnection = connectionProvider.open();
        }
        return writerConnection;
    }

    /** Opens the embedded DuckDB connection owned by one sink writer. */
    @FunctionalInterface
    public interface ConnectionProvider {
        Connection open() throws SQLException;
    }

    /** Loads a table schema when savepoint rescaling creates a writer without local state. */
    @FunctionalInterface
    public interface SchemaProvider extends AutoCloseable {
        Schema load(TableId tableId) throws IOException;

        @Override
        default void close() throws IOException {}
    }
}
