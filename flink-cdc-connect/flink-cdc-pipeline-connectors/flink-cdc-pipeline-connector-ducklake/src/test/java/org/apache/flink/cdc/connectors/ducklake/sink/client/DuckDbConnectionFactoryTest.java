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

package org.apache.flink.cdc.connectors.ducklake.sink.client;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.connectors.ducklake.sink.DuckLakeSinkConfig;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.cdc.connectors.ducklake.testutils.JdbcTestUtils.defaultValue;
import static org.apache.flink.cdc.connectors.ducklake.testutils.JdbcTestUtils.recordingConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DuckDbConnectionFactory}. */
class DuckDbConnectionFactoryTest {

    @Test
    void initializesCatalogConnectionInSafeOrder() throws Exception {
        List<String> statements = new ArrayList<>();
        DuckLakeSinkConfig config = DuckLakeSinkConfig.from(validConfiguration());
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        config, ignored -> recordingConnection(statements), () -> "catalog");

        try (Connection ignored = factory.openCatalogConnection()) {
            assertThat(statements)
                    .containsExactly(
                            "LOAD httpfs",
                            "LOAD postgres",
                            "LOAD ducklake",
                            "CREATE OR REPLACE SECRET \"flink_cdc_ducklake_s3\" "
                                    + "(TYPE s3, PROVIDER config, KEY_ID 'access''key', "
                                    + "SECRET 'secret-key', REGION 'us-east-1', "
                                    + "ENDPOINT 'minio:9000', USE_SSL false, URL_STYLE 'path', "
                                    + "SCOPE 's3://warehouse/ducklake')",
                            "SET threads = 1",
                            "SET memory_limit = '256MB'",
                            "SET temp_directory = '/tmp/flink-cdc-ducklake-test/duckdb-catalog'",
                            "SET preserve_insertion_order = false",
                            "ATTACH 'ducklake:postgres:host=''postgres.example.com'' "
                                    + "port=''5432'' dbname=''ducklake'' user=''ducklake_user'' "
                                    + "password=''ducklake_password'' sslmode=''prefer''' "
                                    + "AS \"ducklake\" (DATA_PATH 's3://warehouse/ducklake', "
                                    + "OVERRIDE_DATA_PATH false)");
        }
    }

    @Test
    void writerConnectionDoesNotAttachCatalog() throws Exception {
        List<String> statements = new ArrayList<>();
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        DuckLakeSinkConfig.from(validConfiguration()),
                        ignored -> recordingConnection(statements),
                        () -> "writer");

        try (Connection ignored = factory.openWriterConnection()) {
            assertThat(statements)
                    .containsExactly(
                            "LOAD httpfs",
                            "CREATE OR REPLACE SECRET \"flink_cdc_ducklake_s3\" "
                                    + "(TYPE s3, PROVIDER config, KEY_ID 'access''key', "
                                    + "SECRET 'secret-key', REGION 'us-east-1', "
                                    + "ENDPOINT 'minio:9000', USE_SSL false, URL_STYLE 'path', "
                                    + "SCOPE 's3://warehouse/ducklake')",
                            "SET threads = 1",
                            "SET memory_limit = '256MB'",
                            "SET temp_directory = '/tmp/flink-cdc-ducklake-test/duckdb-writer'",
                            "SET preserve_insertion_order = false");
            assertThat(statements).noneMatch(sql -> sql.startsWith("ATTACH"));
        }
    }

    @Test
    void doesNotInstallExtensionsThatAreAlreadyLoadable() throws Exception {
        List<String> statements = new ArrayList<>();
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        DuckLakeSinkConfig.from(validConfiguration()),
                        ignored -> recordingConnection(statements),
                        () -> "writer");

        try (Connection ignored = factory.openWriterConnection()) {}

        assertThat(statements).contains("LOAD httpfs");
        assertThat(statements).noneMatch(sql -> sql.startsWith("INSTALL "));
    }

    @Test
    void installsExtensionWhenInitialLoadFails() throws Exception {
        List<String> statements = new ArrayList<>();
        AtomicInteger loadAttempts = new AtomicInteger();
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        DuckLakeSinkConfig.from(validConfiguration()),
                        ignored -> loadFailingConnection(statements, loadAttempts),
                        () -> "writer");

        try (Connection ignored = factory.openWriterConnection()) {}

        assertThat(statements).startsWith("LOAD httpfs", "INSTALL httpfs", "LOAD httpfs");
    }

    @Test
    void retriesConcurrentDuckLakeCatalogBootstrapConflict() throws Exception {
        AtomicInteger openedConnections = new AtomicInteger();
        AtomicInteger closedConnections = new AtomicInteger();
        List<Long> backoffs = new ArrayList<>();
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        DuckLakeSinkConfig.from(validConfiguration()),
                        ignored ->
                                bootstrapConnection(
                                        openedConnections.getAndIncrement() == 0,
                                        closedConnections),
                        () -> "catalog",
                        backoffs::add);

        try (Connection ignored = factory.openCatalogConnection()) {}

        assertThat(openedConnections).hasValue(2);
        assertThat(closedConnections).hasValue(2);
        assertThat(backoffs).containsExactly(100L);
    }

    @Test
    void doesNotRetryUnrelatedCatalogConnectionFailure() {
        AtomicInteger openedConnections = new AtomicInteger();
        List<Long> backoffs = new ArrayList<>();
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        DuckLakeSinkConfig.from(validConfiguration()),
                        ignored -> {
                            openedConnections.incrementAndGet();
                            throw new SQLException("Permission denied while attaching DuckLake");
                        },
                        () -> "catalog",
                        backoffs::add);

        assertThatThrownBy(factory::openCatalogConnection)
                .isInstanceOf(SQLException.class)
                .hasMessage("Permission denied while attaching DuckLake");
        assertThat(openedConnections).hasValue(1);
        assertThat(backoffs).isEmpty();
    }

    @Test
    void assignsAUniqueSpillDirectoryToEveryInMemoryDatabase() throws Exception {
        List<String> firstStatements = new ArrayList<>();
        List<String> secondStatements = new ArrayList<>();
        AtomicInteger connectionIds = new AtomicInteger();
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        DuckLakeSinkConfig.from(validConfiguration()),
                        ignored ->
                                firstStatements.isEmpty()
                                        ? recordingConnection(firstStatements)
                                        : recordingConnection(secondStatements),
                        () -> Integer.toString(connectionIds.getAndIncrement()));

        try (Connection ignored = factory.openWriterConnection()) {}
        try (Connection ignored = factory.openWriterConnection()) {}

        String firstDirectory = tempDirectoryStatement(firstStatements);
        String secondDirectory = tempDirectoryStatement(secondStatements);
        assertThat(firstDirectory)
                .startsWith("SET temp_directory = '/tmp/flink-cdc-ducklake-test/");
        assertThat(secondDirectory)
                .startsWith("SET temp_directory = '/tmp/flink-cdc-ducklake-test/");
        assertThat(firstDirectory).isNotEqualTo(secondDirectory);
    }

    @Test
    void configuresS3CredentialChainAndLoadsAwsExtension() throws Exception {
        Map<String, String> options = new HashMap<>(validConfiguration().toMap());
        options.remove("storage.properties.access-key");
        options.remove("storage.properties.secret-key");
        options.put("storage.properties.credential-provider", "credential-chain");
        options.put("storage.properties.credential-chain", "env;web_identity");
        options.put("storage.properties.profile", "production");
        List<String> statements = new ArrayList<>();
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        DuckLakeSinkConfig.from(Configuration.fromMap(options)),
                        ignored -> recordingConnection(statements),
                        () -> "writer");

        try (Connection ignored = factory.openWriterConnection()) {}

        assertThat(statements).contains("LOAD httpfs", "LOAD aws");
        assertThat(statements)
                .anySatisfy(
                        sql ->
                                assertThat(sql)
                                        .contains(
                                                "PROVIDER credential_chain",
                                                "CHAIN 'env;web_identity'",
                                                "PROFILE 'production'"));
    }

    @Test
    void masksCredentialsFromDiagnosticStrings() {
        DuckLakeSinkConfig config = DuckLakeSinkConfig.from(validConfiguration());
        DuckDbConnectionFactory factory =
                new DuckDbConnectionFactory(
                        config, ignored -> recordingConnection(new ArrayList<>()));

        assertThat(config.toString())
                .doesNotContain("ducklake_password", "access'key", "secret-key");
        assertThat(factory.toString())
                .doesNotContain("ducklake_password", "access'key", "secret-key");
    }

    private static Configuration validConfiguration() {
        Map<String, String> options = new HashMap<>();
        options.put("catalog.properties.type", "postgres");
        options.put("catalog.properties.host", "postgres.example.com");
        options.put("catalog.properties.database", "ducklake");
        options.put("catalog.properties.user", "ducklake_user");
        options.put("catalog.properties.password", "ducklake_password");
        options.put("storage.properties.type", "s3");
        options.put("storage.properties.path", "s3://warehouse/ducklake");
        options.put("storage.properties.endpoint", "http://minio:9000");
        options.put("storage.properties.access-key", "access'key");
        options.put("storage.properties.secret-key", "secret-key");
        options.put("storage.properties.path-style-access", "true");
        options.put("duckdb.memory-limit", "256MB");
        options.put("duckdb.temp-directory", "/tmp/flink-cdc-ducklake-test");
        return Configuration.fromMap(options);
    }

    private static Connection loadFailingConnection(
            List<String> statements, AtomicInteger loadAttempts) {
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                DuckDbConnectionFactoryTest.class.getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("execute")) {
                                        String sql = (String) args[0];
                                        statements.add(sql);
                                        if (sql.equals("LOAD httpfs")
                                                && loadAttempts.getAndIncrement() == 0) {
                                            throw new SQLException("extension is not installed");
                                        }
                                        return true;
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        return (Connection)
                Proxy.newProxyInstance(
                        DuckDbConnectionFactoryTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("createStatement")) {
                                return statement;
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private static Connection bootstrapConnection(
            boolean failAttach, AtomicInteger closedConnections) {
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                DuckDbConnectionFactoryTest.class.getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("execute")
                                            && failAttach
                                            && ((String) args[0]).startsWith("ATTACH")) {
                                        throw new SQLException(
                                                "Catalog Error: duplicate key value violates "
                                                        + "unique constraint");
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        return (Connection)
                Proxy.newProxyInstance(
                        DuckDbConnectionFactoryTest.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("createStatement")) {
                                return statement;
                            }
                            if (method.getName().equals("close")) {
                                closedConnections.incrementAndGet();
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private static String tempDirectoryStatement(List<String> statements) {
        return statements.stream()
                .filter(statement -> statement.startsWith("SET temp_directory"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing temp_directory statement"));
    }
}
