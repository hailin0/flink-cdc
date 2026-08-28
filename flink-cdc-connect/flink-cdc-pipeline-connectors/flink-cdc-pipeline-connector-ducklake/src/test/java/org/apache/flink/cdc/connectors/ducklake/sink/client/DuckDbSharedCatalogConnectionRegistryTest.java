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

import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DuckDbSharedCatalogConnectionRegistry}. */
class DuckDbSharedCatalogConnectionRegistryTest {

    @TempDir Path tempDir;

    @Test
    void sharesOneDuckDbClientAndReleasesItAfterTheLastLease() throws Exception {
        String catalogPath = "catalog-" + UUID.randomUUID();
        AtomicInteger initializations = new AtomicInteger();
        DuckDbSharedCatalogConnectionRegistry.ConnectionInitializer initializer =
                () -> {
                    initializations.incrementAndGet();
                    return (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                };

        try (Connection first =
                        DuckDbSharedCatalogConnectionRegistry.acquire(
                                catalogPath, "configuration", initializer);
                Connection second =
                        DuckDbSharedCatalogConnectionRegistry.acquire(
                                catalogPath, "configuration", initializer)) {
            try (Statement statement = first.createStatement()) {
                statement.execute("CREATE TABLE shared_table (id INTEGER)");
                statement.execute("INSERT INTO shared_table VALUES (42)");
            }
            try (Statement statement = second.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT id FROM shared_table")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getInt(1)).isEqualTo(42);
            }
            first.close();
            first.close();
            assertThat(first.isClosed()).isTrue();
            assertThat(initializations).hasValue(1);
        }

        try (Connection ignored =
                DuckDbSharedCatalogConnectionRegistry.acquire(
                        catalogPath, "configuration", initializer)) {
            assertThat(initializations).hasValue(2);
        }
    }

    @Test
    void rejectsAConflictingConfigurationForAnOpenCatalog() throws Exception {
        String catalogPath = "catalog-" + UUID.randomUUID();
        DuckDbSharedCatalogConnectionRegistry.ConnectionInitializer initializer =
                () -> (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");

        try (Connection ignored =
                DuckDbSharedCatalogConnectionRegistry.acquire(
                        catalogPath, "first-configuration", initializer)) {
            assertThatThrownBy(
                            () ->
                                    DuckDbSharedCatalogConnectionRegistry.acquire(
                                            catalogPath, "second-configuration", initializer))
                    .isInstanceOf(java.sql.SQLException.class)
                    .hasMessageContaining("already open with a different connector configuration");
        }
    }

    @Test
    void resolvesSymbolicLinksBeforeSelectingTheSharedClient() throws Exception {
        Path catalogDirectory = Files.createDirectory(tempDir.resolve("catalog"));
        Path alias = tempDir.resolve("catalog-alias");
        Files.createSymbolicLink(alias, catalogDirectory);
        Path catalog = catalogDirectory.resolve("metadata.ducklake");
        Path catalogAlias = alias.resolve("metadata.ducklake");
        AtomicInteger initializations = new AtomicInteger();
        DuckDbSharedCatalogConnectionRegistry.ConnectionInitializer initializer =
                () -> {
                    initializations.incrementAndGet();
                    return (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                };

        try (Connection first =
                        DuckDbSharedCatalogConnectionRegistry.acquire(
                                catalog.toString(), "configuration", initializer);
                Connection second =
                        DuckDbSharedCatalogConnectionRegistry.acquire(
                                catalogAlias.toString(), "configuration", initializer)) {
            assertThat(initializations).hasValue(1);
        }
    }
}
