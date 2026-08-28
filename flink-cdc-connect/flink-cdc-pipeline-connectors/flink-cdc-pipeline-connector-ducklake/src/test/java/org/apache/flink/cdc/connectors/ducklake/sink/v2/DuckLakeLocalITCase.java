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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.ducklake.sink.DuckLakeMetadataApplier;
import org.apache.flink.cdc.connectors.ducklake.sink.DuckLakeSinkConfig;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckDbConnectionFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Local DuckDB catalog and filesystem integration tests. */
class DuckLakeLocalITCase {

    private static final TableId TABLE_ID = TableId.tableId("sales", "orders");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator GENERATOR =
            new BinaryRecordDataGenerator(SCHEMA.getColumnDataTypes().toArray(new DataType[0]));

    @TempDir Path tempDir;

    @Test
    void writesAndUpdatesPrimaryKeyMirrorInLocalDuckLake() throws Exception {
        DuckLakeSinkConfig config = localConfig();
        DuckDbConnectionFactory connectionFactory = new DuckDbConnectionFactory(config);
        DuckLakeMetadataApplier metadataApplier = new DuckLakeMetadataApplier();
        DuckLakeJdbcCommitExecutor executor = new DuckLakeJdbcCommitExecutor(connectionFactory);
        try {
            metadataApplier.applySchemaChange(new CreateTableEvent(TABLE_ID, SCHEMA));
            commit(
                    executor,
                    writeResult(
                            connectionFactory,
                            1,
                            DataChangeEvent.insertEvent(TABLE_ID, record(1L, "before"))));
            assertThat(queryName(connectionFactory, 1L)).isEqualTo("before");

            commit(
                    executor,
                    writeResult(
                            connectionFactory,
                            2,
                            DataChangeEvent.updateEvent(
                                    TABLE_ID, record(1L, "before"), record(1L, "after"))));

            assertThat(queryName(connectionFactory, 1L)).isEqualTo("after");
        } finally {
            executor.close();
            metadataApplier.close();
        }
    }

    @Test
    void commitsDataAroundRenameInOneCheckpointTransaction() throws Exception {
        DuckLakeSinkConfig config = localConfig();
        DuckDbConnectionFactory connectionFactory = new DuckDbConnectionFactory(config);
        DuckLakeWriter writer =
                new DuckLakeWriter(
                        connectionFactory::openWriterConnection,
                        tempDir.resolve("warehouse").toString(),
                        0,
                        0,
                        ZoneId.of("UTC"),
                        1,
                        "rename-test",
                        "sink",
                        null);
        DuckLakeJdbcCommitExecutor executor = new DuckLakeJdbcCommitExecutor(connectionFactory);
        Schema renamedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("full_name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        try {
            writer.write(new CreateTableEvent(TABLE_ID, SCHEMA), null);
            writer.write(DataChangeEvent.insertEvent(TABLE_ID, record(1L, "before")), null);
            writer.write(
                    new RenameColumnEvent(TABLE_ID, Collections.singletonMap("name", "full_name")),
                    null);
            writer.write(
                    DataChangeEvent.updateEvent(
                            TABLE_ID,
                            record(renamedSchema, 1L, "before"),
                            record(renamedSchema, 1L, "after")),
                    null);

            Collection<DuckLakeCommittable> committables = writer.prepareCommit();
            List<TestingCommitRequest> requests = new ArrayList<>();
            for (DuckLakeCommittable committable : committables) {
                requests.add(new TestingCommitRequest(committable));
            }
            new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests));

            assertThat(queryColumn(connectionFactory, "full_name", 1L)).isEqualTo("after");

            executor.beginCheckpoint("rename-test", "sink", Collections.singletonList(TABLE_ID));
            try {
                AddColumnEvent reuseRenamedSource =
                        new AddColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        AddColumnEvent.last(
                                                Column.physicalColumn(
                                                        "name", DataTypes.STRING()))));
                assertThatThrownBy(
                                () ->
                                        executor.applySchemaChange(
                                                new DuckLakeSchemaChangeResult(
                                                        2, 1, 0, 0, reuseRenamedSource)))
                        .isInstanceOf(UnsupportedSchemaChangeEventException.class)
                        .hasFieldOrPropertyWithValue(
                                "exceptionMessage",
                                "Reusing a dropped DuckLake column name is not supported: name.");
            } finally {
                executor.rollback();
            }
        } finally {
            writer.close();
            executor.close();
        }
    }

    @Test
    void refreshesTargetSchemaAfterDdlInOneCheckpointTransaction() throws Exception {
        TableId tableId = TableId.tableId("sales", "payments");
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("quantity", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        Schema widenedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("amount", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();
        DuckLakeSinkConfig config = localConfig();
        DuckDbConnectionFactory connectionFactory = new DuckDbConnectionFactory(config);
        DuckLakeWriter writer =
                new DuckLakeWriter(
                        connectionFactory::openWriterConnection,
                        tempDir.resolve("warehouse").toString(),
                        0,
                        0,
                        ZoneId.of("UTC"),
                        1,
                        "alter-test",
                        "sink",
                        null);
        DuckLakeJdbcCommitExecutor executor = new DuckLakeJdbcCommitExecutor(connectionFactory);
        try {
            writer.write(new CreateTableEvent(tableId, initialSchema), null);
            writer.write(
                    DataChangeEvent.insertEvent(
                            tableId, record(initialSchema, 1L, Integer.valueOf(10))),
                    null);
            writer.write(
                    new AlterColumnTypeEvent(
                            tableId,
                            Collections.singletonMap("amount", DataTypes.BIGINT()),
                            Collections.singletonMap("amount", DataTypes.INT())),
                    null);
            writer.write(
                    DataChangeEvent.updateEvent(
                            tableId,
                            record(widenedSchema, 1L, 10L),
                            record(widenedSchema, 1L, 20L)),
                    null);

            List<TestingCommitRequest> requests = new ArrayList<>();
            for (DuckLakeCommittable committable : writer.prepareCommit()) {
                requests.add(new TestingCommitRequest(committable));
            }
            new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests));

            assertThat(queryLong(connectionFactory, tableId, "amount", 1L)).isEqualTo(20L);
        } finally {
            writer.close();
            executor.close();
        }
    }

    @Test
    void preservesWidenedValuesAcrossColumnRename() throws Exception {
        TableId tableId = TableId.tableId("sales", "rename_after_alter");
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING().notNull())
                        .physicalColumn("quantity", DataTypes.INT())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP(3).notNull())
                        .primaryKey("id")
                        .build();
        Schema schemaWithCategory =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING().notNull())
                        .physicalColumn("quantity", DataTypes.INT())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP(3).notNull())
                        .physicalColumn("category", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema widenedSchemaWithCategory =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING().notNull())
                        .physicalColumn("quantity", DataTypes.BIGINT())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP(3).notNull())
                        .physicalColumn("category", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema schemaAfterDrop =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING().notNull())
                        .physicalColumn("quantity", DataTypes.BIGINT())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP(3).notNull())
                        .primaryKey("id")
                        .build();
        Schema renamedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("product_name", DataTypes.STRING().notNull())
                        .physicalColumn("quantity", DataTypes.BIGINT())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP(3).notNull())
                        .primaryKey("id")
                        .build();
        TimestampData createdAt =
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2024-01-01T12:34:56.123"));
        DuckLakeSinkConfig config = localConfig();
        DuckDbConnectionFactory connectionFactory = new DuckDbConnectionFactory(config);
        List<DuckLakeWriter> writers =
                Arrays.asList(
                        writer(config, connectionFactory, 0, "rename-after-alter-test"),
                        writer(config, connectionFactory, 1, "rename-after-alter-test"),
                        writer(config, connectionFactory, 2, "rename-after-alter-test"),
                        writer(config, connectionFactory, 3, "rename-after-alter-test"));
        DuckLakeCommitter committer =
                new DuckLakeCommitter(
                        new DuckLakeJdbcCommitExecutor(connectionFactory), 0, Duration.ZERO);
        try {
            writeSchemaChange(writers, new CreateTableEvent(tableId, initialSchema));
            writers.get(0)
                    .write(
                            DataChangeEvent.insertEvent(
                                    tableId, record(initialSchema, 3, "gamma", 30, createdAt)),
                            null);
            writers.get(1)
                    .write(
                            DataChangeEvent.insertEvent(
                                    tableId, record(initialSchema, 2, "beta", 20, createdAt)),
                            null);
            writers.get(2)
                    .write(
                            DataChangeEvent.insertEvent(
                                    tableId, record(initialSchema, 1, "alpha", 10, createdAt)),
                            null);
            writers.get(3)
                    .write(
                            DataChangeEvent.insertEvent(
                                    tableId, record(initialSchema, 4, "delta", 40, createdAt)),
                            null);
            commit(committer, prepareCommit(writers));

            for (DuckLakeWriter writer : writers) {
                writer.close();
            }
            committer.close();
            writers =
                    Arrays.asList(
                            writer(config, connectionFactory, 0, "restored-rename-test"),
                            writer(config, connectionFactory, 1, "restored-rename-test"),
                            writer(config, connectionFactory, 2, "restored-rename-test"),
                            writer(config, connectionFactory, 3, "restored-rename-test"));
            committer =
                    new DuckLakeCommitter(
                            new DuckLakeJdbcCommitExecutor(connectionFactory), 0, Duration.ZERO);

            writers.get(2)
                    .write(
                            DataChangeEvent.insertEvent(
                                    tableId, record(initialSchema, 5, "epsilon", 50, createdAt)),
                            null);
            writers.get(1)
                    .write(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    record(initialSchema, 2, "beta", 20, createdAt),
                                    record(initialSchema, 2, "beta-v1", 20, createdAt)),
                            null);
            writers.get(1)
                    .write(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    record(initialSchema, 2, "beta-v1", 20, createdAt),
                                    record(initialSchema, 2, "beta-v2", 20, createdAt)),
                            null);
            writers.get(2)
                    .write(
                            DataChangeEvent.deleteEvent(
                                    tableId, record(initialSchema, 1, "alpha", 10, createdAt)),
                            null);
            writeSchemaChange(
                    writers,
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    AddColumnEvent.last(
                                            Column.physicalColumn(
                                                    "category", DataTypes.STRING())))));
            writers.get(1)
                    .write(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    record(schemaWithCategory, 2, "beta-v2", 20, createdAt, null),
                                    record(
                                            schemaWithCategory,
                                            2,
                                            "beta-v2",
                                            20,
                                            createdAt,
                                            "priority")),
                            null);
            writeSchemaChange(
                    writers,
                    new AlterColumnTypeEvent(
                            tableId,
                            Collections.singletonMap("quantity", DataTypes.BIGINT()),
                            Collections.singletonMap("quantity", DataTypes.INT())));
            writers.get(0)
                    .write(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    record(
                                            widenedSchemaWithCategory,
                                            3,
                                            "gamma",
                                            30L,
                                            createdAt,
                                            null),
                                    record(
                                            widenedSchemaWithCategory,
                                            3,
                                            "gamma",
                                            3_000_000_000L,
                                            createdAt,
                                            null)),
                            null);
            commit(committer, prepareCommit(writers));

            assertThat(queryQuantityBeforeRename(connectionFactory, tableId, 3L))
                    .isEqualTo(3_000_000_000L);

            writeSchemaChange(
                    writers, new DropColumnEvent(tableId, Collections.singletonList("category")));
            commit(committer, prepareCommit(writers));

            assertThat(queryQuantityBeforeRename(connectionFactory, tableId, 3L))
                    .isEqualTo(3_000_000_000L);

            writers.get(1)
                    .write(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    record(schemaAfterDrop, 2, "beta-v2", 20L, createdAt),
                                    record(
                                            schemaAfterDrop,
                                            2,
                                            "beta-before-rename",
                                            20L,
                                            createdAt)),
                            null);
            writeSchemaChange(
                    writers,
                    new RenameColumnEvent(
                            tableId, Collections.singletonMap("name", "product_name")));
            writers.get(1)
                    .write(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    record(renamedSchema, 2, "beta-before-rename", 20L, createdAt),
                                    record(renamedSchema, 2, "beta-after-rename", 20L, createdAt)),
                            null);
            commit(committer, prepareCommit(writers));

            assertThat(queryColumn(connectionFactory, tableId, "product_name", 2L))
                    .isEqualTo("beta-after-rename");
            assertThat(queryQuantityFromFullProjection(connectionFactory, tableId, 3L))
                    .isEqualTo(3_000_000_000L);
        } finally {
            for (DuckLakeWriter writer : writers) {
                writer.close();
            }
            committer.close();
        }
    }

    @Test
    void recoversWhenDuckLakeCommitsBeforeFlinkObservesSuccess() throws Exception {
        DuckLakeSinkConfig config = localConfig();
        DuckDbConnectionFactory connectionFactory = new DuckDbConnectionFactory(config);
        DuckLakeMetadataApplier metadataApplier = new DuckLakeMetadataApplier();
        CommitSucceededThenFailedExecutor executor =
                new CommitSucceededThenFailedExecutor(
                        new DuckLakeJdbcCommitExecutor(connectionFactory));
        try {
            metadataApplier.applySchemaChange(new CreateTableEvent(TABLE_ID, SCHEMA));
            DuckLakeWriteResult writeResult =
                    writeResult(
                            connectionFactory,
                            1,
                            DataChangeEvent.insertEvent(TABLE_ID, record(1L, "once")));
            DuckLakeCommittable committable =
                    new DuckLakeCommittable(
                            "local-fault-test",
                            "sink",
                            writeResult,
                            DuckLakeCommittableSerializer.hashWriteResult(writeResult));
            TestingCommitRequest request = new TestingCommitRequest(committable);
            TestingCommitRequest createRequest = createRequest("local-fault-test", 1);

            new DuckLakeCommitter(executor, 1, Duration.ZERO)
                    .commit(new ArrayList<>(Arrays.asList(createRequest, request)));

            assertThat(executor.applyCalls).hasValue(1);
            assertThat(request.alreadyCommitted).isTrue();
            assertThat(queryName(connectionFactory, 1L)).isEqualTo("once");
        } finally {
            executor.close();
            metadataApplier.close();
        }
    }

    @Test
    void appliesRepeatedPrimaryKeyAcrossSuccessiveFileSegments() throws Exception {
        DuckLakeSinkConfig config = localConfig();
        DuckDbConnectionFactory connectionFactory = new DuckDbConnectionFactory(config);
        DuckLakeMetadataApplier metadataApplier = new DuckLakeMetadataApplier();
        DuckLakeJdbcCommitExecutor executor = new DuckLakeJdbcCommitExecutor(connectionFactory);
        try {
            metadataApplier.applySchemaChange(new CreateTableEvent(TABLE_ID, SCHEMA));
            DuckLakeWriteResult before =
                    writeResult(
                            connectionFactory,
                            1,
                            0,
                            DataChangeEvent.insertEvent(TABLE_ID, record(1L, "before")));
            DuckLakeWriteResult after =
                    writeResult(
                            connectionFactory,
                            1,
                            1,
                            DataChangeEvent.updateEvent(
                                    TABLE_ID, record(1L, "before"), record(1L, "after")));
            List<TestingCommitRequest> requests =
                    Arrays.asList(
                            createRequest("segmented-test", 1),
                            request("segmented-test", before),
                            request("segmented-test", after));

            new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests));

            assertThat(queryName(connectionFactory, 1L)).isEqualTo("after");
        } finally {
            executor.close();
            metadataApplier.close();
        }
    }

    @Test
    void rejectsDifferentGlobalDuckDbSettingsForTheSameOpenCatalog() throws Exception {
        DuckLakeSinkConfig firstConfig = localConfig();
        DuckDbConnectionFactory firstFactory = new DuckDbConnectionFactory(firstConfig);
        Map<String, String> differentOptions = new HashMap<>();
        differentOptions.put("catalog.properties.type", "duckdb");
        differentOptions.put(
                "catalog.properties.path", tempDir.resolve("catalog/metadata.ducklake").toString());
        differentOptions.put("storage.properties.type", "filesystem");
        differentOptions.put("storage.properties.path", tempDir.resolve("warehouse").toString());
        differentOptions.put("duckdb.memory-limit", "1GB");
        differentOptions.put("duckdb.temp-directory", tempDir.resolve("other-tmp").toString());
        DuckDbConnectionFactory differentFactory =
                new DuckDbConnectionFactory(
                        DuckLakeSinkConfig.from(
                                Configuration.fromMap(differentOptions), ZoneId.of("UTC")));

        try (Connection ignored = firstFactory.openCatalogConnection()) {
            assertThatThrownBy(differentFactory::openCatalogConnection)
                    .isInstanceOf(java.sql.SQLException.class)
                    .hasMessageContaining("already open with a different connector configuration");
        }
    }

    @Test
    void serializesConcurrentCommittersForTheSameCheckpoint() throws Exception {
        DuckLakeSinkConfig config = localConfig();
        DuckDbConnectionFactory connectionFactory = new DuckDbConnectionFactory(config);
        DuckLakeMetadataApplier metadataApplier = new DuckLakeMetadataApplier();
        DuckLakeJdbcCommitExecutor firstExecutor =
                new DuckLakeJdbcCommitExecutor(connectionFactory);
        DuckLakeJdbcCommitExecutor secondExecutor =
                new DuckLakeJdbcCommitExecutor(connectionFactory);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            metadataApplier.applySchemaChange(new CreateTableEvent(TABLE_ID, SCHEMA));
            DuckLakeWriteResult writeResult =
                    writeResult(
                            connectionFactory,
                            1,
                            DataChangeEvent.insertEvent(TABLE_ID, record(1L, "once")));
            TestingCommitRequest firstRequest = request("concurrent-test", writeResult);
            TestingCommitRequest secondRequest = request("concurrent-test", writeResult);
            List<TestingCommitRequest> firstRequests =
                    Arrays.asList(createRequest("concurrent-test", 1), firstRequest);
            List<TestingCommitRequest> secondRequests =
                    Arrays.asList(createRequest("concurrent-test", 1), secondRequest);
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);

            Future<?> firstCommit =
                    threads.submit(
                            () -> {
                                ready.countDown();
                                start.await();
                                new DuckLakeCommitter(firstExecutor, 10, Duration.ofMillis(10))
                                        .commit(new ArrayList<>(firstRequests));
                                return null;
                            });
            Future<?> secondCommit =
                    threads.submit(
                            () -> {
                                ready.countDown();
                                start.await();
                                new DuckLakeCommitter(secondExecutor, 10, Duration.ofMillis(10))
                                        .commit(new ArrayList<>(secondRequests));
                                return null;
                            });

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            firstCommit.get(60, TimeUnit.SECONDS);
            secondCommit.get(60, TimeUnit.SECONDS);

            assertThat(Arrays.asList(firstRequest, secondRequest))
                    .filteredOn(request -> request.alreadyCommitted)
                    .hasSize(1);
            assertThat(queryCommitCount(connectionFactory, "concurrent-test", 1)).isEqualTo(1);
            assertThat(queryName(connectionFactory, 1L)).isEqualTo("once");
        } finally {
            threads.shutdownNow();
            firstExecutor.close();
            secondExecutor.close();
            metadataApplier.close();
        }
    }

    private DuckLakeWriteResult writeResult(
            DuckDbConnectionFactory connectionFactory, long checkpointId, DataChangeEvent event)
            throws Exception {
        return writeResult(connectionFactory, checkpointId, 0, event);
    }

    private DuckLakeWriteResult writeResult(
            DuckDbConnectionFactory connectionFactory,
            long checkpointId,
            long fileSequence,
            DataChangeEvent event)
            throws Exception {
        Path dataPath =
                tempDir.resolve(
                        "warehouse/sales/orders/flink-cdc/checkpoint-"
                                + checkpointId
                                + "/data-"
                                + fileSequence
                                + ".parquet");
        Path keyPath =
                tempDir.resolve(
                        "warehouse/_flink_cdc_staging/keys/checkpoint-"
                                + checkpointId
                                + "/keys-"
                                + fileSequence
                                + ".parquet");
        java.nio.file.Files.createDirectories(dataPath.getParent());
        java.nio.file.Files.createDirectories(keyPath.getParent());
        try (DuckLakeTableBuffer buffer =
                new DuckLakeTableBuffer(
                        connectionFactory.openWriterConnection(),
                        TABLE_ID,
                        SCHEMA,
                        ZoneId.of("UTC"))) {
            buffer.append(event);
            return buffer.flushToFiles(
                    checkpointId, 0, 0, 0, fileSequence, dataPath.toString(), keyPath.toString());
        }
    }

    private static TestingCommitRequest request(String sinkId, DuckLakeWriteResult writeResult)
            throws Exception {
        return new TestingCommitRequest(
                new DuckLakeCommittable(
                        sinkId,
                        "sink",
                        writeResult,
                        DuckLakeCommittableSerializer.hashWriteResult(writeResult)));
    }

    private static TestingCommitRequest createRequest(String sinkId, long checkpointId)
            throws Exception {
        DuckLakeSchemaChangeResult change =
                new DuckLakeSchemaChangeResult(
                        checkpointId, 0, 0, 0, new CreateTableEvent(TABLE_ID, SCHEMA));
        return new TestingCommitRequest(
                new DuckLakeCommittable(
                        sinkId,
                        "sink",
                        change,
                        DuckLakeCommittableSerializer.hashSchemaChangeResult(change)));
    }

    private static void commit(DuckLakeJdbcCommitExecutor executor, DuckLakeWriteResult writeResult)
            throws Exception {
        List<TestingCommitRequest> requests = new ArrayList<>();
        if (writeResult.getCheckpointId() == 1) {
            requests.add(createRequest("local-test", 1));
        }
        requests.add(request("local-test", writeResult));
        new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests));
    }

    private static void commit(
            DuckLakeCommitter committer, Collection<DuckLakeCommittable> committables)
            throws Exception {
        List<TestingCommitRequest> requests = new ArrayList<>();
        for (DuckLakeCommittable committable : committables) {
            requests.add(new TestingCommitRequest(committable));
        }
        committer.commit(new ArrayList<>(requests));
    }

    private DuckLakeWriter writer(
            DuckLakeSinkConfig config,
            DuckDbConnectionFactory connectionFactory,
            int subtaskId,
            String sinkId) {
        return new DuckLakeWriter(
                connectionFactory::openWriterConnection,
                new DuckLakeCatalogSchemaProvider(connectionFactory),
                config.getStoragePath(),
                subtaskId,
                0,
                ZoneId.of("UTC"),
                1,
                sinkId,
                "sink",
                null);
    }

    private static void writeSchemaChange(List<DuckLakeWriter> writers, SchemaChangeEvent event)
            throws Exception {
        for (DuckLakeWriter writer : writers) {
            writer.write(event, null);
        }
    }

    private static Collection<DuckLakeCommittable> prepareCommit(List<DuckLakeWriter> writers)
            throws Exception {
        List<DuckLakeCommittable> committables = new ArrayList<>();
        for (DuckLakeWriter writer : writers) {
            committables.addAll(writer.prepareCommit());
        }
        return committables;
    }

    private static String queryName(DuckDbConnectionFactory connectionFactory, long id)
            throws Exception {
        return queryColumn(connectionFactory, "name", id);
    }

    private static String queryColumn(
            DuckDbConnectionFactory connectionFactory, String column, long id) throws Exception {
        return queryColumn(connectionFactory, TABLE_ID, column, id);
    }

    private static String queryColumn(
            DuckDbConnectionFactory connectionFactory, TableId tableId, String column, long id)
            throws Exception {
        try (Connection connection = connectionFactory.openCatalogConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT \""
                                        + column.replace("\"", "\"\"")
                                        + "\" FROM ducklake.\""
                                        + tableId.getSchemaName().replace("\"", "\"\"")
                                        + "\".\""
                                        + tableId.getTableName().replace("\"", "\"\"")
                                        + "\" WHERE id = "
                                        + id)) {
            assertThat(resultSet.next()).isTrue();
            String name = resultSet.getString(1);
            assertThat(resultSet.next()).isFalse();
            return name;
        }
    }

    private static long queryLong(
            DuckDbConnectionFactory connectionFactory, TableId tableId, String column, long id)
            throws Exception {
        try (Connection connection = connectionFactory.openCatalogConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT \""
                                        + column.replace("\"", "\"\"")
                                        + "\" FROM ducklake.\""
                                        + tableId.getSchemaName().replace("\"", "\"\"")
                                        + "\".\""
                                        + tableId.getTableName().replace("\"", "\"\"")
                                        + "\" WHERE id = "
                                        + id)) {
            assertThat(resultSet.next()).isTrue();
            long value = resultSet.getLong(1);
            assertThat(resultSet.next()).isFalse();
            return value;
        }
    }

    private static long queryQuantityFromFullProjection(
            DuckDbConnectionFactory connectionFactory, TableId tableId, long id) throws Exception {
        try (Connection connection = connectionFactory.openCatalogConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT id, product_name, quantity, created_at FROM ducklake.\""
                                        + tableId.getSchemaName().replace("\"", "\"\"")
                                        + "\".\""
                                        + tableId.getTableName().replace("\"", "\"\"")
                                        + "\" ORDER BY id")) {
            while (resultSet.next()) {
                if (resultSet.getLong(1) == id) {
                    return resultSet.getLong(3);
                }
            }
            throw new SQLException("Missing row with id " + id);
        }
    }

    private static long queryQuantityBeforeRename(
            DuckDbConnectionFactory connectionFactory, TableId tableId, long id) throws Exception {
        try (Connection connection = connectionFactory.openCatalogConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT id, name, quantity, created_at FROM ducklake.\""
                                        + tableId.getSchemaName().replace("\"", "\"\"")
                                        + "\".\""
                                        + tableId.getTableName().replace("\"", "\"\"")
                                        + "\" ORDER BY id")) {
            while (resultSet.next()) {
                if (resultSet.getLong(1) == id) {
                    return resultSet.getLong(3);
                }
            }
            throw new SQLException("Missing row with id " + id);
        }
    }

    private static long queryCommitCount(
            DuckDbConnectionFactory connectionFactory, String sinkId, long checkpointId)
            throws Exception {
        try (Connection connection = connectionFactory.openCatalogConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT count(*) FROM ducklake._flink_cdc_internal.commits "
                                        + "WHERE sink_instance_id = '"
                                        + sinkId
                                        + "' AND operator_id = 'sink' AND checkpoint_id = "
                                        + checkpointId)) {
            assertThat(resultSet.next()).isTrue();
            return resultSet.getLong(1);
        }
    }

    private DuckLakeSinkConfig localConfig() {
        Map<String, String> options = new HashMap<>();
        options.put("catalog.properties.type", "duckdb");
        options.put(
                "catalog.properties.path", tempDir.resolve("catalog/metadata.ducklake").toString());
        options.put("storage.properties.type", "filesystem");
        options.put("storage.properties.path", tempDir.resolve("warehouse").toString());
        options.put("duckdb.temp-directory", tempDir.resolve("tmp").toString());
        return DuckLakeSinkConfig.from(Configuration.fromMap(options), ZoneId.of("UTC"));
    }

    private static RecordData record(long id, String name) {
        return GENERATOR.generate(new Object[] {id, BinaryStringData.fromString(name)});
    }

    private static RecordData record(Schema schema, long id, String name) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        return generator.generate(new Object[] {id, BinaryStringData.fromString(name)});
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

    private static final class TestingCommitRequest
            implements Committer.CommitRequest<DuckLakeCommittable> {
        private final DuckLakeCommittable committable;
        private boolean alreadyCommitted;

        private TestingCommitRequest(DuckLakeCommittable committable) {
            this.committable = committable;
        }

        @Override
        public DuckLakeCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable throwable) {}

        @Override
        public void signalFailedWithUnknownReason(Throwable throwable) {}

        @Override
        public void retryLater() {}

        @Override
        public void updateAndRetryLater(DuckLakeCommittable committable) {}

        @Override
        public void signalAlreadyCommitted() {
            alreadyCommitted = true;
        }
    }

    private static final class CommitSucceededThenFailedExecutor implements DuckLakeCommitExecutor {
        private final DuckLakeCommitExecutor delegate;
        private final AtomicInteger applyCalls = new AtomicInteger();
        private boolean failAfterCommit = true;

        private CommitSucceededThenFailedExecutor(DuckLakeCommitExecutor delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<String> findCommittedPlanHash(
                String sinkId, String operatorId, long checkpointId) throws SQLException {
            return delegate.findCommittedPlanHash(sinkId, operatorId, checkpointId);
        }

        @Override
        public void beginCheckpoint(String sinkId, String operatorId, List<TableId> tables)
                throws SQLException {
            delegate.beginCheckpoint(sinkId, operatorId, tables);
        }

        @Override
        public void applyTableChanges(List<DuckLakeCommittable> committables) throws SQLException {
            applyCalls.incrementAndGet();
            delegate.applyTableChanges(committables);
        }

        @Override
        public void applySchemaChange(DuckLakeSchemaChangeResult schemaChangeResult)
                throws SQLException {
            delegate.applySchemaChange(schemaChangeResult);
        }

        @Override
        public void recordCheckpoint(
                String sinkId, String operatorId, long checkpointId, String planHash)
                throws SQLException {
            delegate.recordCheckpoint(sinkId, operatorId, checkpointId, planHash);
        }

        @Override
        public void commit() throws SQLException {
            delegate.commit();
            if (failAfterCommit) {
                failAfterCommit = false;
                throw new SQLException("connection lost after commit", "08006");
            }
        }

        @Override
        public void rollback() throws SQLException {
            delegate.rollback();
        }

        @Override
        public boolean isRetryable(SQLException exception) {
            return delegate.isRetryable(exception);
        }

        @Override
        public void resetConnection() throws SQLException {
            delegate.resetConnection();
        }

        @Override
        public void cleanupOrphanFiles(Duration retention) throws SQLException {
            delegate.cleanupOrphanFiles(retention);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
