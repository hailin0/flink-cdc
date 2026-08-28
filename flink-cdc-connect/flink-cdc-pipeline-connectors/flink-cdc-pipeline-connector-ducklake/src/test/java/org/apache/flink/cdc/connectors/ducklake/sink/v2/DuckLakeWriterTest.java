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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeWriterTest {

    private static final TableId TABLE_ID = TableId.tableId("sales", "orders");
    private static final String SINK_ID = "sink-instance-1";
    private static final String OPERATOR_ID = "operator-9";
    private static final Schema INITIAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @TempDir Path tempDir;

    @Test
    void writesSnapshotInsertsForCreatedTable() throws Exception {
        DuckLakeWriter writer = newWriter();
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 2L, "b")), null);

        Collection<DuckLakeCommittable> committables = writer.prepareCommit();

        assertThat(committables).hasSize(1);
        DuckLakeCommittable committable = committables.iterator().next();
        assertThat(committable.getCheckpointId()).isEqualTo(1);
        assertThat(committable.getSchemaBatchIndex()).isZero();
        assertThat(committable.getFileSequence()).isZero();
        assertThat(committable.getWriteResult().getDataRowCount()).isEqualTo(2);
        assertThat(committable.getWriteResult().getKeyRowCount()).isEqualTo(2);
        assertThat(committable.getWriteResult().getDataFilePath())
                .contains("/sales/orders/flink-cdc/")
                .doesNotContain("/_flink_cdc_data/");
        assertThat(committable.getWriteResult().getKeyFilePath())
                .contains("/_flink_cdc_staging/keys/");
        assertThat(committable.getWriteResultHash()).hasSize(64);
    }

    @Test
    void usesUniquePathsForRepeatedCheckpointMaterialization() throws Exception {
        DuckLakeWriter firstWriter = newWriter();
        firstWriter.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        firstWriter.write(
                DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "first")), null);
        DuckLakeCommittable first = firstWriter.prepareCommit().iterator().next();

        DuckLakeWriter secondWriter = newWriter();
        secondWriter.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        secondWriter.write(
                DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "second")), null);
        DuckLakeCommittable second = secondWriter.prepareCommit().iterator().next();

        assertThat(second.getWriteResult().getDataFilePath())
                .isNotEqualTo(first.getWriteResult().getDataFilePath());
        assertThat(second.getWriteResult().getKeyFilePath())
                .isNotEqualTo(first.getWriteResult().getKeyFilePath());
        assertThat(second.getWriteResultHash()).isNotEqualTo(first.getWriteResultHash());
    }

    @Test
    void acceptsRepeatedIdenticalCreateTableEvent() throws Exception {
        DuckLakeWriter writer = newWriter();
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(new CreateTableEvent(TABLE_ID, identicalSchema()), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);

        Collection<DuckLakeCommittable> committables = writer.prepareCommit();

        assertThat(committables).hasSize(1);
        assertThat(committables.iterator().next().getWriteResult().getDataRowCount()).isEqualTo(1);
    }

    @Test
    void loadsSchemaForAWriterAddedBySavepointRescaling() throws Exception {
        DuckLakeWriter writer =
                new DuckLakeWriter(
                        () -> DriverManager.getConnection("jdbc:duckdb:"),
                        tableId -> INITIAL_SCHEMA,
                        tempDir.toString(),
                        1,
                        0,
                        ZoneId.of("UTC"),
                        6,
                        SINK_ID,
                        OPERATOR_ID,
                        null);

        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);

        assertThat(writer.prepareCommit()).hasSize(1);
        assertThat(writer.snapshotState(6).get(0).getCurrentSchemas())
                .containsEntry(TABLE_ID, INITIAL_SCHEMA);
    }

    @Test
    void loadsAlreadyAppliedSchemaChangeForARescaledWriter() throws Exception {
        AddColumnEvent addColumn = addExtraColumn();
        Schema appliedSchema = SchemaUtils.applySchemaChangeEvent(INITIAL_SCHEMA, addColumn);
        DuckLakeWriter writer =
                new DuckLakeWriter(
                        () -> DriverManager.getConnection("jdbc:duckdb:"),
                        tableId -> appliedSchema,
                        tempDir.toString(),
                        1,
                        0,
                        ZoneId.of("UTC"),
                        6,
                        SINK_ID,
                        OPERATOR_ID,
                        null);

        writer.write(addColumn, null);
        writer.write(
                DataChangeEvent.insertEvent(TABLE_ID, record(appliedSchema, 1L, "a", "loaded")),
                null);

        DuckLakeCommittable committable = writer.prepareCommit().iterator().next();
        assertThat(committable.getSchemaBatchIndex()).isEqualTo(1);
        assertThat(committable.getWriteResult().getColumns())
                .extracting(DuckLakeColumnMetadata::getName)
                .containsExactly("id", "name", "extra");
    }

    private static Schema identicalSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("name", DataTypes.STRING())
                .primaryKey("id")
                .build();
    }

    @Test
    void keepsOneDeduplicatedFilePerCheckpointSchemaBatch() throws Exception {
        DuckLakeWriter writer = newWriter();
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);
        writer.write(
                DataChangeEvent.updateEvent(
                        TABLE_ID, record(INITIAL_SCHEMA, 1L, "a"), record(INITIAL_SCHEMA, 1L, "b")),
                null);

        List<DuckLakeCommittable> committables = sorted(writer.prepareCommit());

        assertThat(committables).hasSize(1);
        assertThat(committables.get(0).getSchemaBatchIndex()).isZero();
        assertThat(committables.get(0).getFileSequence()).isZero();
        assertThat(committables.get(0).getWriteResult().getDataRowCount()).isEqualTo(1);
    }

    @Test
    void broadcastSchemaChangeAdvancesBatchEvenWithoutBufferedEvents() throws Exception {
        DuckLakeWriter writer0 = newWriter(0);
        DuckLakeWriter writer1 = newWriter(1);
        CreateTableEvent create = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        writer0.write(create, null);
        writer1.write(create, null);
        writer0.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);

        AddColumnEvent addColumn = addExtraColumn();
        writer0.write(addColumn, null);
        writer1.write(addColumn, null);
        Schema newSchema = SchemaUtils.applySchemaChangeEvent(INITIAL_SCHEMA, addColumn);
        writer0.write(DataChangeEvent.insertEvent(TABLE_ID, record(newSchema, 2L, "b", "x")), null);
        writer1.write(DataChangeEvent.insertEvent(TABLE_ID, record(newSchema, 3L, "c", "y")), null);

        List<DuckLakeCommittable> writer0Commits = sorted(writer0.prepareCommit());
        List<DuckLakeCommittable> writer1Commits = sorted(writer1.prepareCommit());

        assertThat(writer0Commits)
                .extracting(DuckLakeCommittable::getSchemaBatchIndex)
                .containsExactly(0, 1);
        assertThat(writer1Commits)
                .extracting(DuckLakeCommittable::getSchemaBatchIndex)
                .containsExactly(1);
    }

    @Test
    void restorePreservesStableIdsCheckpointAndBatchState() throws Exception {
        DuckLakeWriter writer = newWriter();
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        AddColumnEvent addColumn = addExtraColumn();
        writer.write(addColumn, null);
        DuckLakeWriterState state = writer.snapshotState(0).get(0);

        DuckLakeWriter restored = restoredWriter(state, 7);
        Schema newSchema = SchemaUtils.applySchemaChangeEvent(INITIAL_SCHEMA, addColumn);
        restored.write(
                DataChangeEvent.insertEvent(TABLE_ID, record(newSchema, 1L, "a", "x")), null);
        DuckLakeCommittable committable = restored.prepareCommit().iterator().next();

        assertThat(committable.getSinkId()).isEqualTo(SINK_ID);
        assertThat(committable.getOperatorId()).isEqualTo(OPERATOR_ID);
        assertThat(committable.getCheckpointId()).isEqualTo(1);
        assertThat(committable.getSchemaBatchIndex()).isEqualTo(1);
        assertThat(committable.getSubtaskId()).isEqualTo(7);
    }

    @Test
    void prepareCommitClosesBuffersAndAdvancesWriterEpoch() throws Exception {
        DuckLakeWriter writer = newWriter();
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);

        Collection<DuckLakeCommittable> first = writer.prepareCommit();
        Collection<DuckLakeCommittable> second = writer.prepareCommit();
        DuckLakeWriterState state = writer.snapshotState(1).get(0);

        assertThat(first).hasSize(1);
        assertThat(second).isEmpty();
        assertThat(state.getWriterEpoch()).isEqualTo(3);
        assertThat(state.getPendingWriteResults()).isEmpty();
    }

    @Test
    void flushClosesCurrentBufferBeforeSchemaCoordinationContinues() throws Exception {
        DuckLakeWriter writer = newWriter();
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);

        writer.flush(false);

        DuckLakeWriterState state = writer.snapshotState(1).get(0);
        assertThat(state.getPendingWriteResults()).hasSize(1);
        assertThat(state.getPendingWriteResults().get(0).getDataRowCount()).isEqualTo(1);
    }

    @Test
    void globalFlushSeparatesLaterChangesForEveryFlushedTable() throws Exception {
        TableId customers = TableId.tableId("sales", "customers");
        DuckLakeWriter writer = newWriter();
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(new CreateTableEvent(customers, INITIAL_SCHEMA), null);
        writer.write(
                DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "before")), null);

        writer.flush(false);
        writer.write(addColumn(customers), null);
        writer.write(
                DataChangeEvent.updateEvent(
                        TABLE_ID,
                        record(INITIAL_SCHEMA, 1L, "before"),
                        record(INITIAL_SCHEMA, 1L, "after")),
                null);

        List<DuckLakeCommittable> committables = sorted(writer.prepareCommit());
        assertThat(committables)
                .filteredOn(committable -> committable.getTableId().equals(TABLE_ID))
                .extracting(DuckLakeCommittable::getSchemaBatchIndex)
                .containsExactly(0, 0);
        assertThat(committables)
                .filteredOn(committable -> committable.getTableId().equals(TABLE_ID))
                .extracting(DuckLakeCommittable::getFileSequence)
                .containsExactly(0L, 1L);
    }

    @Test
    void sharesOneEmbeddedDuckDbConnectionAcrossTables() throws Exception {
        AtomicInteger openedConnections = new AtomicInteger();
        DuckLakeWriter writer =
                new DuckLakeWriter(
                        () -> {
                            openedConnections.incrementAndGet();
                            return DriverManager.getConnection("jdbc:duckdb:");
                        },
                        tempDir.toString(),
                        0,
                        2,
                        ZoneId.of("UTC"),
                        1,
                        SINK_ID,
                        OPERATOR_ID,
                        null);
        TableId customers = TableId.tableId("sales", "customers");
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(new CreateTableEvent(customers, INITIAL_SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);
        writer.write(DataChangeEvent.insertEvent(customers, record(INITIAL_SCHEMA, 2L, "b")), null);

        assertThat(writer.prepareCommit()).hasSize(2);
        assertThat(openedConnections).hasValue(1);
    }

    @Test
    void rollsFilesBeforeCheckpointAtConfiguredRowLimit() throws Exception {
        DuckLakeWriter writer =
                new DuckLakeWriter(
                        () -> DriverManager.getConnection("jdbc:duckdb:"),
                        tableId -> INITIAL_SCHEMA,
                        tempDir.toString(),
                        0,
                        0,
                        ZoneId.of("UTC"),
                        1,
                        SINK_ID,
                        OPERATOR_ID,
                        null,
                        2);
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 2L, "b")), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 3L, "c")), null);

        List<DuckLakeCommittable> committables = sorted(writer.prepareCommit());

        assertThat(committables).hasSize(2);
        assertThat(committables)
                .extracting(DuckLakeCommittable::getFileSequence)
                .containsExactly(0L, 1L);
        assertThat(committables)
                .extracting(committable -> committable.getWriteResult().getDataRowCount())
                .containsExactly(2L, 1L);
    }

    @Test
    void rollsLeastRecentlyUsedTableAtGlobalBufferedRowLimit() throws Exception {
        TableId customers = TableId.tableId("sales", "customers");
        DuckLakeWriter writer = newWriterWithLimits(Long.MAX_VALUE, 3, 10);
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        writer.write(new CreateTableEvent(customers, INITIAL_SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 1L, "a")), null);
        writer.write(DataChangeEvent.insertEvent(customers, record(INITIAL_SCHEMA, 2L, "b")), null);
        writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(INITIAL_SCHEMA, 3L, "c")), null);

        List<DuckLakeWriteResult> rolled = writer.snapshotState(1).get(0).getPendingWriteResults();

        assertThat(rolled)
                .singleElement()
                .extracting(DuckLakeWriteResult::getTableId)
                .isEqualTo(customers);
        assertThat(rolled.get(0).getDataRowCount()).isEqualTo(1);
    }

    @Test
    void rollsLeastRecentlyUsedTableBeforeOpeningTooManyBuffers() throws Exception {
        TableId customers = TableId.tableId("sales", "customers");
        TableId products = TableId.tableId("sales", "products");
        DuckLakeWriter writer = newWriterWithLimits(Long.MAX_VALUE, Long.MAX_VALUE, 2);
        for (TableId tableId : Arrays.asList(TABLE_ID, customers, products)) {
            writer.write(new CreateTableEvent(tableId, INITIAL_SCHEMA), null);
            writer.write(
                    DataChangeEvent.insertEvent(
                            tableId, record(INITIAL_SCHEMA, (long) tableId.hashCode(), "row")),
                    null);
        }

        List<DuckLakeWriteResult> rolled = writer.snapshotState(1).get(0).getPendingWriteResults();

        assertThat(rolled)
                .singleElement()
                .extracting(DuckLakeWriteResult::getTableId)
                .isEqualTo(TABLE_ID);
    }

    @Test
    void rejectsTableWithoutPrimaryKeyBeforeWritingFiles() throws Exception {
        DuckLakeWriter writer = newWriter();
        Schema noKey = Schema.newBuilder().physicalColumn("id", DataTypes.BIGINT()).build();

        assertThatThrownBy(() -> writer.write(new CreateTableEvent(TABLE_ID, noKey), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key");
        assertThat(parquetFiles()).isEmpty();
    }

    @Test
    void rejectsPrimaryKeyChangingUpdateBeforeWritingFiles() throws Exception {
        DuckLakeWriter writer = newWriter();
        writer.write(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA), null);
        DataChangeEvent keyChange =
                DataChangeEvent.updateEvent(
                        TABLE_ID, record(INITIAL_SCHEMA, 1L, "a"), record(INITIAL_SCHEMA, 2L, "b"));
        assertThatThrownBy(() -> writer.write(keyChange, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Primary-key-changing UPDATE is not supported");
        assertThat(parquetFiles()).isEmpty();
    }

    @Test
    void rejectsThreePartTargetTableBeforeWritingFiles() throws Exception {
        DuckLakeWriter writer = newWriter();
        TableId threePartTable = TableId.tableId("catalog", "sales", "orders");

        assertThatThrownBy(
                        () ->
                                writer.write(
                                        new CreateTableEvent(threePartTable, INITIAL_SCHEMA), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Three-part target table identifiers are not supported");
        assertThat(parquetFiles()).isEmpty();
    }

    @Test
    void rejectsSchemaThatCollidesWithStagingStoragePrefix() throws Exception {
        DuckLakeWriter writer = newWriter();
        TableId reserved = TableId.tableId("_flink_cdc_staging", "keys");

        assertThatThrownBy(() -> writer.write(new CreateTableEvent(reserved, INITIAL_SCHEMA), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("_flink_cdc_staging")
                .hasMessageContaining("reserved");
        assertThat(parquetFiles()).isEmpty();
    }

    @Test
    void encodesSchemaAndTableNamesInDataPaths() throws Exception {
        DuckLakeWriter writer = newWriter();
        TableId specialTable = TableId.tableId("sales/..", "order name");
        writer.write(new CreateTableEvent(specialTable, INITIAL_SCHEMA), null);
        writer.write(
                DataChangeEvent.insertEvent(specialTable, record(INITIAL_SCHEMA, 1L, "encoded")),
                null);

        DuckLakeCommittable committable = writer.prepareCommit().iterator().next();

        assertThat(committable.getWriteResult().getDataFilePath())
                .contains("/sales%2F%2E%2E/order%20name/flink-cdc/")
                .doesNotContain("/sales/../");
    }

    private DuckLakeWriter newWriter() {
        return newWriter(0);
    }

    private DuckLakeWriter newWriter(int subtaskId) {
        return new DuckLakeWriter(
                () -> DriverManager.getConnection("jdbc:duckdb:"),
                tempDir.toString(),
                subtaskId,
                2,
                ZoneId.of("UTC"),
                1,
                SINK_ID,
                OPERATOR_ID,
                null);
    }

    private DuckLakeWriter newWriterWithLimits(
            long maxEventsPerFile, long maxBufferedEvents, int maxOpenTables) {
        return new DuckLakeWriter(
                () -> DriverManager.getConnection("jdbc:duckdb:"),
                tableId -> INITIAL_SCHEMA,
                tempDir.toString(),
                0,
                0,
                ZoneId.of("UTC"),
                1,
                SINK_ID,
                OPERATOR_ID,
                null,
                maxEventsPerFile,
                maxBufferedEvents,
                maxOpenTables);
    }

    private DuckLakeWriter restoredWriter(DuckLakeWriterState state, int subtaskId) {
        return new DuckLakeWriter(
                () -> DriverManager.getConnection("jdbc:duckdb:"),
                tempDir.toString(),
                subtaskId,
                3,
                ZoneId.of("UTC"),
                state.getWriterEpoch(),
                state.getSinkId(),
                state.getOperatorId(),
                state);
    }

    private static AddColumnEvent addExtraColumn() {
        return addColumn(TABLE_ID);
    }

    private static AddColumnEvent addColumn(TableId tableId) {
        return new AddColumnEvent(
                tableId,
                Collections.singletonList(
                        AddColumnEvent.last(Column.physicalColumn("extra", DataTypes.STRING()))));
    }

    private static RecordData record(Schema schema, Object... values) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        Object[] converted = values.clone();
        for (int i = 0; i < converted.length; i++) {
            if (converted[i] instanceof String) {
                converted[i] = BinaryStringData.fromString((String) converted[i]);
            }
        }
        return generator.generate(converted);
    }

    private static List<DuckLakeCommittable> sorted(Collection<DuckLakeCommittable> committables) {
        return committables.stream()
                .sorted(Comparator.comparingLong(DuckLakeCommittable::getFileSequence))
                .collect(Collectors.toList());
    }

    private List<Path> parquetFiles() throws Exception {
        try (Stream<Path> paths = Files.walk(tempDir)) {
            return paths.filter(path -> path.toString().endsWith(".parquet"))
                    .collect(Collectors.toList());
        }
    }
}
