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
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
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
        DuckLakeMetadataApplier metadataApplier = new DuckLakeMetadataApplier(config);
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
    void recoversWhenDuckLakeCommitsBeforeFlinkObservesSuccess() throws Exception {
        DuckLakeSinkConfig config = localConfig();
        DuckDbConnectionFactory connectionFactory = new DuckDbConnectionFactory(config);
        DuckLakeMetadataApplier metadataApplier = new DuckLakeMetadataApplier(config);
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

            new DuckLakeCommitter(executor, 1, Duration.ZERO)
                    .commit(Collections.singletonList(request));

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
        DuckLakeMetadataApplier metadataApplier = new DuckLakeMetadataApplier(config);
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
                            request("segmented-test", before), request("segmented-test", after));

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
        DuckLakeMetadataApplier metadataApplier = new DuckLakeMetadataApplier(config);
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
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);

            Future<?> firstCommit =
                    threads.submit(
                            () -> {
                                ready.countDown();
                                start.await();
                                new DuckLakeCommitter(firstExecutor, 10, Duration.ofMillis(10))
                                        .commit(Collections.singletonList(firstRequest));
                                return null;
                            });
            Future<?> secondCommit =
                    threads.submit(
                            () -> {
                                ready.countDown();
                                start.await();
                                new DuckLakeCommitter(secondExecutor, 10, Duration.ofMillis(10))
                                        .commit(Collections.singletonList(secondRequest));
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

    private static void commit(DuckLakeJdbcCommitExecutor executor, DuckLakeWriteResult writeResult)
            throws Exception {
        DuckLakeCommittable committable =
                new DuckLakeCommittable(
                        "local-test",
                        "sink",
                        writeResult,
                        DuckLakeCommittableSerializer.hashWriteResult(writeResult));
        executor.beginCheckpoint("local-test", "sink", Collections.singletonList(TABLE_ID));
        executor.applyTableChanges(Collections.singletonList(committable));
        executor.recordCheckpoint(
                "local-test",
                "sink",
                writeResult.getCheckpointId(),
                DuckLakeCommittableSerializer.hashWriteResult(writeResult));
        executor.commit();
    }

    private static String queryName(DuckDbConnectionFactory connectionFactory, long id)
            throws Exception {
        try (Connection connection = connectionFactory.openCatalogConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT name FROM ducklake.sales.orders WHERE id = " + id)) {
            assertThat(resultSet.next()).isTrue();
            String name = resultSet.getString(1);
            assertThat(resultSet.next()).isFalse();
            return name;
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
