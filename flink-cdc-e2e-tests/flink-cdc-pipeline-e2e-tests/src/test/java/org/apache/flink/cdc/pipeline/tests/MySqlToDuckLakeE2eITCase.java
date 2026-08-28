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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** End-to-end tests for MySQL CDC to DuckLake pipeline jobs. */
class MySqlToDuckLakeE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlToDuckLakeE2eITCase.class);

    private static final String POSTGRES_ALIAS = "ducklake-postgres";
    private static final String POSTGRES_DATABASE = "ducklake";
    private static final String POSTGRES_USER = "ducklake";
    private static final String POSTGRES_PASSWORD = "ducklake-password";
    private static final String MINIO_ALIAS = "ducklake-minio";
    private static final String MINIO_ACCESS_KEY = "ducklake-access-key";
    private static final String MINIO_SECRET_KEY = "ducklake-secret-key";
    private static final String STORAGE_PATH = "s3://warehouse/ducklake";
    private static final String TABLE_NAME = "ducklake_products";
    private static final Duration DUCKLAKE_RESULT_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration DUCKLAKE_QUERY_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration DUCKLAKE_RETRY_INTERVAL = Duration.ofSeconds(2);
    private static final String DUCKLAKE_CONNECTOR_IN_CONTAINER =
            "/tmp/cdc/SNAPSHOT/lib/ducklake-cdc-pipeline-connector.jar";
    private static final String QUERY_TOOL_ROOT = "/tmp/ducklake-query";

    @Container
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:17.11"))
                    .withDatabaseName(POSTGRES_DATABASE)
                    .withUsername(POSTGRES_USER)
                    .withPassword(POSTGRES_PASSWORD)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(POSTGRES_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Container
    private static final GenericContainer<?> MINIO =
            new GenericContainer<>(
                            DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"))
                    .withCommand("server", "/data")
                    .withEnv("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
                    .withEnv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(MINIO_ALIAS)
                    .withExposedPorts(9000)
                    .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000));

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Override
    @BeforeEach
    public void before() throws Exception {
        super.before();
        inventoryDatabase.createAndInitialize();
        MINIO.execInContainer("mkdir", "-p", "/data/warehouse");
        createSourceTable();
    }

    @Override
    @AfterEach
    public void after() {
        try {
            inventoryDatabase.dropDatabase();
        } finally {
            super.after();
        }
    }

    @Test
    void testSnapshotDmlAndSchemaEvolution() throws Exception {
        String database = inventoryDatabase.getDatabaseName();
        Path duckLakeConnector = TestUtils.getResource("ducklake-cdc-pipeline-connector.jar");

        JobID jobId = submitPipelineJob(pipelineJob(database, parallelism), duckLakeConnector);
        waitUntilJobRunning(jobId, Duration.ofMinutes(2));
        copyDuckLakeQueryTool();

        waitForRows(database, initialRows());
        waitForColumnType(database, "created_at", "TIMESTAMP_MS");
        long initialDataSnapshot = waitForLatestFlinkCdcSnapshot(database);

        String savepointPath = stopJobWithSavepoint(jobId);
        int restoredParallelism = parallelism == 1 ? 2 : parallelism;
        jobId =
                submitPipelineJob(
                        pipelineJob(database, restoredParallelism),
                        savepointPath,
                        false,
                        duckLakeConnector);
        waitUntilJobRunning(jobId, Duration.ofMinutes(2));

        applyIncrementalChanges();
        triggerCheckpointWithRetry(jobId);

        waitForRows(
                database,
                Arrays.asList(
                        "2, beta-v2, 20, 2024-01-01 12:34:56.123, priority",
                        "3, gamma, 3000000000, 2024-01-01 12:34:56.123, null",
                        "4, delta, 40, 2024-01-01 12:34:56.123, null",
                        "5, epsilon, 50, 2024-01-01 12:34:56.123, null"));
        waitForColumnType(database, "quantity", "BIGINT");
        waitForRowsAtSnapshot(database, initialDataSnapshot, initialRows());

        dropCategoryColumn();
        triggerCheckpointWithRetry(jobId);
        waitForRows(
                database,
                Arrays.asList(
                        "2, beta-v2, 20, 2024-01-01 12:34:56.123",
                        "3, gamma, 3000000000, 2024-01-01 12:34:56.123",
                        "4, delta, 40, 2024-01-01 12:34:56.123",
                        "5, epsilon, 50, 2024-01-01 12:34:56.123"));

        renameNameColumnAndUpdate();
        triggerCheckpointWithRetry(jobId);
        waitForRows(
                database,
                Arrays.asList(
                        "2, 20, 2024-01-01 12:34:56.123, beta-after-rename",
                        "3, 3000000000, 2024-01-01 12:34:56.123, gamma",
                        "4, 40, 2024-01-01 12:34:56.123, delta",
                        "5, 50, 2024-01-01 12:34:56.123, epsilon"));
        waitForColumnType(database, "product_name", "VARCHAR");
        waitForColumnAbsent(database, "name");
        waitUntilJobRunning(jobId, Duration.ofMinutes(2));
    }

    private String pipelineJob(String database, int jobParallelism) {
        return String.format(
                "source:\n"
                        + "  type: mysql\n"
                        + "  hostname: mysql\n"
                        + "  port: 3306\n"
                        + "  username: %s\n"
                        + "  password: %s\n"
                        + "  tables: %s.%s\n"
                        + "  server-id: 5400-5404\n"
                        + "  server-time-zone: UTC\n"
                        + "  scan.incremental.snapshot.chunk.size: 2\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: ducklake\n"
                        + "  catalog.properties.type: postgres\n"
                        + "  catalog.properties.host: %s\n"
                        + "  catalog.properties.port: 5432\n"
                        + "  catalog.properties.database: %s\n"
                        + "  catalog.properties.user: %s\n"
                        + "  catalog.properties.password: %s\n"
                        + "  catalog.properties.ssl-mode: disable\n"
                        + "  storage.properties.type: s3\n"
                        + "  storage.properties.path: %s\n"
                        + "  storage.properties.endpoint: http://%s:9000\n"
                        + "  storage.properties.region: us-east-1\n"
                        + "  storage.properties.access-key: %s\n"
                        + "  storage.properties.secret-key: %s\n"
                        + "  storage.properties.path-style-access: true\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  schema.change.behavior: evolve\n"
                        + "  parallelism: %s\n",
                MYSQL_TEST_USER,
                MYSQL_TEST_PASSWORD,
                database,
                TABLE_NAME,
                POSTGRES_ALIAS,
                POSTGRES_DATABASE,
                POSTGRES_USER,
                POSTGRES_PASSWORD,
                STORAGE_PATH,
                MINIO_ALIAS,
                MINIO_ACCESS_KEY,
                MINIO_SECRET_KEY,
                jobParallelism);
    }

    private void createSourceTable() throws SQLException {
        try (Connection connection = openMySqlConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE "
                            + TABLE_NAME
                            + " ("
                            + "id INT NOT NULL, "
                            + "name VARCHAR(64) NOT NULL, "
                            + "quantity INT, "
                            + "created_at DATETIME(3) NOT NULL, "
                            + "PRIMARY KEY (id))");
            statement.execute(
                    "INSERT INTO "
                            + TABLE_NAME
                            + " VALUES "
                            + "(1, 'alpha', 10, '2024-01-01 12:34:56.123'), "
                            + "(2, 'beta', 20, '2024-01-01 12:34:56.123'), "
                            + "(3, 'gamma', 30, '2024-01-01 12:34:56.123'), "
                            + "(4, 'delta', 40, '2024-01-01 12:34:56.123')");
        }
    }

    private void applyIncrementalChanges() throws SQLException {
        try (Connection connection = openMySqlConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO "
                            + TABLE_NAME
                            + " VALUES (5, 'epsilon', 50, '2024-01-01 12:34:56.123')");
            statement.execute("UPDATE " + TABLE_NAME + " SET name = 'beta-v1' WHERE id = 2");
            statement.execute("UPDATE " + TABLE_NAME + " SET name = 'beta-v2' WHERE id = 2");
            statement.execute("DELETE FROM " + TABLE_NAME + " WHERE id = 1");
            statement.execute("ALTER TABLE " + TABLE_NAME + " ADD COLUMN category VARCHAR(32)");
            statement.execute("UPDATE " + TABLE_NAME + " SET category = 'priority' WHERE id = 2");
            statement.execute("ALTER TABLE " + TABLE_NAME + " MODIFY COLUMN quantity BIGINT");
            statement.execute("UPDATE " + TABLE_NAME + " SET quantity = 3000000000 WHERE id = 3");
        }
    }

    private void dropCategoryColumn() throws SQLException {
        try (Connection connection = openMySqlConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE " + TABLE_NAME + " DROP COLUMN category");
        }
    }

    private void renameNameColumnAndUpdate() throws SQLException {
        try (Connection connection = openMySqlConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE " + TABLE_NAME + " SET name = 'beta-before-rename' WHERE id = 2");
            statement.execute("ALTER TABLE " + TABLE_NAME + " RENAME COLUMN name TO product_name");
            statement.execute(
                    "UPDATE "
                            + TABLE_NAME
                            + " SET product_name = 'beta-after-rename' WHERE id = 2");
        }
    }

    private Connection openMySqlConnection() throws SQLException {
        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        inventoryDatabase.getDatabaseName());
        return DriverManager.getConnection(jdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
    }

    private void copyDuckLakeQueryTool() {
        String classResource = DuckLakeQueryTool.class.getName().replace('.', '/') + ".class";
        jobManager.copyFileToContainer(
                MountableFile.forClasspathResource(classResource),
                QUERY_TOOL_ROOT + "/" + classResource);
    }

    private void waitForRows(String database, List<String> expected) throws Exception {
        waitUntilAsserted(
                () ->
                        Assertions.assertThat(runDuckLakeQuery("rows", database, ""))
                                .containsExactlyElementsOf(expected));
    }

    private void waitForColumnType(String database, String columnName, String expectedType)
            throws Exception {
        waitUntilAsserted(
                () ->
                        Assertions.assertThat(runDuckLakeQuery("column-type", database, columnName))
                                .containsExactly(expectedType));
    }

    private void waitForColumnAbsent(String database, String columnName) throws Exception {
        waitUntilAsserted(
                () ->
                        Assertions.assertThat(runDuckLakeQuery("column-type", database, columnName))
                                .isEmpty());
    }

    private void waitForRowsAtSnapshot(String database, long snapshotId, List<String> expected)
            throws Exception {
        waitUntilAsserted(
                () ->
                        Assertions.assertThat(
                                        runDuckLakeQuery(
                                                "rows-at-snapshot",
                                                database,
                                                Long.toString(snapshotId)))
                                .containsExactlyElementsOf(expected));
    }

    private long waitForLatestFlinkCdcSnapshot(String database) throws Exception {
        long[] snapshotId = {-1};
        waitUntilAsserted(
                () -> {
                    List<String> snapshots = runDuckLakeQuery("flink-cdc-snapshots", database, "");
                    Assertions.assertThat(snapshots)
                            .isNotEmpty()
                            .allSatisfy(
                                    snapshot ->
                                            Assertions.assertThat(snapshot)
                                                    .contains("Flink CDC checkpoint")
                                                    .contains("primary-key-current-state"));
                    snapshotId[0] =
                            Long.parseLong(
                                    snapshots
                                            .get(snapshots.size() - 1)
                                            .substring(
                                                    0,
                                                    snapshots
                                                            .get(snapshots.size() - 1)
                                                            .indexOf('|')));
                });
        return snapshotId[0];
    }

    private static List<String> initialRows() {
        return Arrays.asList(
                "1, alpha, 10, 2024-01-01 12:34:56.123",
                "2, beta, 20, 2024-01-01 12:34:56.123",
                "3, gamma, 30, 2024-01-01 12:34:56.123",
                "4, delta, 40, 2024-01-01 12:34:56.123");
    }

    private List<String> runDuckLakeQuery(String mode, String database, String columnName)
            throws IOException, SQLException, InterruptedException {
        ExecResult result =
                jobManager.execInContainer(
                        "timeout",
                        DUCKLAKE_QUERY_TIMEOUT.getSeconds() + "s",
                        "java",
                        "-cp",
                        DUCKLAKE_CONNECTOR_IN_CONTAINER + ":" + QUERY_TOOL_ROOT,
                        DuckLakeQueryTool.class.getName(),
                        mode,
                        POSTGRES_ALIAS,
                        "5432",
                        POSTGRES_DATABASE,
                        POSTGRES_USER,
                        POSTGRES_PASSWORD,
                        STORAGE_PATH,
                        MINIO_ALIAS + ":9000",
                        MINIO_ACCESS_KEY,
                        MINIO_SECRET_KEY,
                        database,
                        TABLE_NAME,
                        columnName);
        if (result.getExitCode() != 0) {
            throw new SQLException("DuckLake query failed: " + result.getStderr());
        }
        return result.getStdout()
                .lines()
                .filter(line -> line.startsWith(DuckLakeQueryTool.OUTPUT_PREFIX))
                .map(line -> line.substring(DuckLakeQueryTool.OUTPUT_PREFIX.length()))
                .collect(Collectors.toList());
    }

    private void waitUntilAsserted(CheckedAssertion assertion) throws Exception {
        long deadline = System.nanoTime() + DUCKLAKE_RESULT_TIMEOUT.toNanos();
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                assertion.run();
                return;
            } catch (AssertionError | IOException | SQLException e) {
                lastFailure = e;
                Thread.sleep(DUCKLAKE_RETRY_INTERVAL.toMillis());
            }
        }
        throw new AssertionError(
                "DuckLake result did not converge. JobManager logs:\n"
                        + logTail(jobManagerConsumer.toUtf8String())
                        + "\nTaskManager logs:\n"
                        + logTail(taskManagerConsumer.toUtf8String()),
                lastFailure);
    }

    private static String logTail(String logs) {
        int maximumLength = 20_000;
        return logs.substring(Math.max(0, logs.length() - maximumLength));
    }

    private void waitUntilJobRunning(JobID jobId, Duration timeout) throws Exception {
        Deadline deadline = Deadline.fromNow(timeout);
        while (deadline.hasTimeLeft()) {
            JobStatus status = getRestClusterClient().getJobStatus(jobId).get(10, TimeUnit.SECONDS);
            if (status == JobStatus.RUNNING) {
                return;
            }
            if (status.isTerminalState()) {
                throw new IllegalStateException(
                        "Restored DuckLake job terminated with status " + status);
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Restored DuckLake job did not reach RUNNING state");
    }

    @FunctionalInterface
    private interface CheckedAssertion {
        void run() throws IOException, SQLException, InterruptedException;
    }
}
