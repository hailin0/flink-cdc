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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.v2.DuckLakeSink;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DuckLakeDataSinkFactory}. */
class DuckLakeDataSinkFactoryTest {

    @TempDir Path tempDir;

    @Test
    void discoversDuckLakeFactory() {
        DataSinkFactory factory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("ducklake", DataSinkFactory.class);

        assertThat(factory).isInstanceOf(DuckLakeDataSinkFactory.class);
    }

    @Test
    void createsDuckLakeDataSinkFromValidConfiguration() {
        DataSink dataSink = createSink(validConfiguration());

        assertThat(dataSink).isInstanceOf(DuckLakeDataSink.class);
        assertThat(dataSink.getEventSinkProvider()).isInstanceOf(FlinkSinkProvider.class);
        assertThat(((FlinkSinkProvider) dataSink.getEventSinkProvider()).getSink())
                .isInstanceOf(DuckLakeSink.class);
    }

    @Test
    void rejectsLocalStoragePath() {
        Map<String, String> options = validConfiguration().toMap();
        options.put("storage.properties.path", "file:///tmp/ducklake");
        Configuration configuration = Configuration.fromMap(options);

        assertThatThrownBy(() -> createSink(configuration))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("storage.properties.path must use s3://");
    }

    @Test
    void acceptsDuckDbCatalogWithLocalFilesystem() {
        Configuration configuration =
                localConfiguration(
                        tempDir.resolve("catalog.ducklake"), tempDir.resolve("warehouse"));

        assertThat(createSink(configuration)).isInstanceOf(DuckLakeDataSink.class);
    }

    @Test
    void rejectsDuckDbCatalogWithRemoteStorage() {
        Map<String, String> options = new HashMap<>();
        options.put("catalog.properties.type", "duckdb");
        options.put("catalog.properties.path", tempDir.resolve("catalog.ducklake").toString());
        options.put("storage.properties.type", "s3");
        options.put("storage.properties.path", "s3://warehouse/ducklake");
        options.put("storage.properties.access-key", "access-key");
        options.put("storage.properties.secret-key", "secret-key");

        assertThatThrownBy(() -> createSink(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "catalog.properties.type=duckdb is supported only with "
                                + "storage.properties.type=filesystem");
    }

    @Test
    void requiresSharedFilesystemAcknowledgementWithPostgresCatalog() {
        Map<String, String> options = validConfiguration().toMap();
        options.put("storage.properties.type", "filesystem");
        options.put("storage.properties.path", tempDir.resolve("warehouse").toString());
        options.remove("storage.properties.access-key");
        options.remove("storage.properties.secret-key");

        assertThatThrownBy(() -> createSink(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "storage.properties.shared must be true with a non-DuckDB catalog");

        options.put("storage.properties.shared", "true");
        assertThat(createSink(Configuration.fromMap(options))).isNotNull();
    }

    @Test
    void rejectsS3StoragePathWithoutBucket() {
        Map<String, String> options = validConfiguration().toMap();
        options.put("storage.properties.path", "s3:///ducklake");

        assertThatThrownBy(() -> createSink(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("storage.properties.path must contain an S3 bucket");
    }

    @Test
    void rejectsS3EndpointWithPath() {
        Map<String, String> options = validConfiguration().toMap();
        options.put("storage.properties.endpoint", "http://minio:9000/api");

        assertThatThrownBy(() -> createSink(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("storage.properties.endpoint must not contain a path");
    }

    @Test
    void requiresProviderTypes() {
        assertThatThrownBy(() -> createSink(new Configuration()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("catalog.properties.type")
                .hasMessageContaining("storage.properties.type");
    }

    @Test
    void rejectsUnsupportedProviderType() {
        Map<String, String> options = validConfiguration().toMap();
        options.put("catalog.properties.type", "sqlite");

        assertThatThrownBy(() -> createSink(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported catalog.properties.type 'sqlite'")
                .hasMessageContaining("postgres");
    }

    @Test
    void rejectsUnknownProviderProperty() {
        Map<String, String> options = validConfiguration().toMap();
        options.put("storage.properties.unknown", "value");

        assertThatThrownBy(() -> createSink(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported option 'storage.properties.unknown'");
    }

    @Test
    void reportsMissingProviderPropertyWithFullOptionName() {
        Map<String, String> options = validConfiguration().toMap();
        options.remove("catalog.properties.password");

        assertThatThrownBy(() -> createSink(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("catalog.properties.password");
    }

    @Test
    void rejectsLegacyFlatProviderOptions() {
        Map<String, String> options = validConfiguration().toMap();
        options.put("catalog.host", "legacy.example.com");

        assertThatThrownBy(() -> createSink(Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("catalog.host");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidSinkOptions")
    void rejectsInvalidSinkOption(
            String testCase, Consumer<Configuration> invalidOption, String expectedMessage) {
        Configuration configuration = validConfiguration();
        invalidOption.accept(configuration);

        assertThatThrownBy(() -> createSink(configuration))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    void usesConservativeOrphanCleanupDefaults() {
        DuckLakeSinkConfig config = DuckLakeSinkConfig.from(validConfiguration());

        assertThat(config.getOrphanCleanupInterval()).isZero();
        assertThat(config.getOrphanRetention()).isEqualTo(Duration.ofDays(7));
        assertThat(config.getWriterMaxBufferedEvents()).isEqualTo(1_000_000L);
        assertThat(config.getWriterMaxOpenTables()).isEqualTo(128);
    }

    @Test
    void acceptsS3CredentialChainWithoutStaticKeys() {
        Map<String, String> options = new HashMap<>(validConfiguration().toMap());
        options.remove("storage.properties.access-key");
        options.remove("storage.properties.secret-key");
        options.put("storage.properties.credential-provider", "credential-chain");
        options.put("storage.properties.credential-chain", "env;web_identity");

        assertThat(createSink(Configuration.fromMap(options))).isNotNull();
    }

    @Test
    void masksExtensionRepositoryCredentialsFromDiagnosticStrings() {
        Configuration configuration = validConfiguration();
        configuration.set(
                DuckLakeDataSinkOptions.DUCKDB_EXTENSION_REPOSITORY,
                "https://repository-user:repository-password@example.com/extensions?token=secret");

        String diagnostic = createSink(configuration).toString();

        assertThat(diagnostic)
                .doesNotContain(
                        "repository-user",
                        "repository-password",
                        "token=secret",
                        "ducklake_password",
                        "access-key",
                        "secret-key")
                .contains("extensionRepository=<configured>");
    }

    private static Stream<Arguments> invalidSinkOptions() {
        return Stream.of(
                Arguments.of(
                        "negative commit retries",
                        (Consumer<Configuration>)
                                config ->
                                        config.set(
                                                DuckLakeDataSinkOptions.SINK_COMMIT_MAX_RETRIES,
                                                -1),
                        "sink.commit.max-retries must not be negative"),
                Arguments.of(
                        "non-positive writer file event limit",
                        (Consumer<Configuration>)
                                config ->
                                        config.set(
                                                DuckLakeDataSinkOptions
                                                        .SINK_WRITER_MAX_EVENTS_PER_FILE,
                                                0L),
                        "sink.writer.max-events-per-file must be positive"),
                Arguments.of(
                        "non-positive global writer event limit",
                        (Consumer<Configuration>)
                                config ->
                                        config.set(
                                                DuckLakeDataSinkOptions
                                                        .SINK_WRITER_MAX_BUFFERED_EVENTS,
                                                0L),
                        "sink.writer.max-buffered-events must be positive"),
                Arguments.of(
                        "non-positive open table limit",
                        (Consumer<Configuration>)
                                config ->
                                        config.set(
                                                DuckLakeDataSinkOptions.SINK_WRITER_MAX_OPEN_TABLES,
                                                0),
                        "sink.writer.max-open-tables must be positive"),
                Arguments.of(
                        "negative orphan cleanup interval",
                        (Consumer<Configuration>)
                                config ->
                                        config.set(
                                                DuckLakeDataSinkOptions
                                                        .SINK_ORPHAN_CLEANUP_INTERVAL,
                                                Duration.ofSeconds(-1)),
                        "sink.orphan-cleanup.interval must not be negative"),
                Arguments.of(
                        "non-positive orphan retention",
                        (Consumer<Configuration>)
                                config ->
                                        config.set(
                                                DuckLakeDataSinkOptions.SINK_ORPHAN_RETENTION,
                                                Duration.ZERO),
                        "sink.orphan-cleanup.retention must be positive"));
    }

    private static DataSink createSink(Configuration configuration) {
        DataSinkFactory factory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("ducklake", DataSinkFactory.class);
        return factory.createDataSink(
                new FactoryHelper.DefaultContext(
                        configuration,
                        configuration,
                        Thread.currentThread().getContextClassLoader()));
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
        options.put("storage.properties.access-key", "access-key");
        options.put("storage.properties.secret-key", "secret-key");
        return Configuration.fromMap(options);
    }

    private static Configuration localConfiguration(Path catalogPath, Path storagePath) {
        Map<String, String> options = new HashMap<>();
        options.put("catalog.properties.type", "duckdb");
        options.put("catalog.properties.path", catalogPath.toString());
        options.put("storage.properties.type", "filesystem");
        options.put("storage.properties.path", storagePath.toString());
        return Configuration.fromMap(options);
    }
}
