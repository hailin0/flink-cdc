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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.ducklake.sink.DuckLakeSinkConfig;
import org.apache.flink.cdc.connectors.ducklake.sink.provider.DuckDbCatalogProviderFactory;
import org.apache.flink.cdc.connectors.ducklake.sink.provider.DuckDbExtension;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeRetryBackoff;

import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/** Opens isolated in-memory DuckDB connections for writers and catalog operations. */
@Internal
public final class DuckDbConnectionFactory {

    private static final String JDBC_URL = "jdbc:duckdb:";
    private static final String DUCKLAKE_CATALOG = DuckLakeCatalogObjects.CATALOG_ALIAS;
    private static final int CATALOG_BOOTSTRAP_MAX_ATTEMPTS = 8;
    private static final long CATALOG_BOOTSTRAP_INITIAL_BACKOFF_MILLIS = 100L;

    private final DuckLakeSinkConfig config;
    private final ConnectionProvider connectionProvider;
    private final Supplier<String> connectionIdSupplier;
    private final DuckLakeRetryBackoff.Sleeper sleeper;

    public DuckDbConnectionFactory(DuckLakeSinkConfig config) {
        this(config, DriverManager::getConnection);
    }

    DuckDbConnectionFactory(DuckLakeSinkConfig config, ConnectionProvider connectionProvider) {
        this(config, connectionProvider, () -> UUID.randomUUID().toString(), Thread::sleep);
    }

    DuckDbConnectionFactory(
            DuckLakeSinkConfig config,
            ConnectionProvider connectionProvider,
            Supplier<String> connectionIdSupplier) {
        this(config, connectionProvider, connectionIdSupplier, Thread::sleep);
    }

    DuckDbConnectionFactory(
            DuckLakeSinkConfig config,
            ConnectionProvider connectionProvider,
            Supplier<String> connectionIdSupplier,
            DuckLakeRetryBackoff.Sleeper sleeper) {
        this.config = config;
        this.connectionProvider = connectionProvider;
        this.connectionIdSupplier = connectionIdSupplier;
        this.sleeper = sleeper;
    }

    public Connection openWriterConnection() throws SQLException {
        Connection connection = connectionProvider.open(JDBC_URL);
        try {
            configureExtensionLocations(connection);
            installAndLoad(connection, config.getStorageProvider().requiredExtensions());
            config.getStorageProvider().configure(connection);
            configureLocalResources(connection);
            return connection;
        } catch (SQLException | RuntimeException e) {
            closeAfterInitializationFailure(connection, e);
            throw e;
        }
    }

    public Connection openCatalogConnection() throws SQLException {
        if (DuckDbCatalogProviderFactory.TYPE.equals(config.getCatalogProvider().type())) {
            return openSharedDuckDbCatalogConnection();
        }
        return openCatalogConnectionWithRetry();
    }

    private Connection openSharedDuckDbCatalogConnection() throws SQLException {
        return DuckDbSharedCatalogConnectionRegistry.acquire(
                config.getCatalogProvider().metadataPath(),
                localCatalogConfigurationFingerprint(),
                this::openDuckDbCatalogRootConnection);
    }

    private DuckDBConnection openDuckDbCatalogRootConnection() throws SQLException {
        Connection connection = openCatalogConnectionWithRetry();
        if (connection instanceof DuckDBConnection) {
            return (DuckDBConnection) connection;
        }
        SQLException failure = new SQLException("DuckDB catalog requires a DuckDB JDBC connection");
        closeAfterInitializationFailure(connection, failure);
        throw failure;
    }

    private Connection openCatalogConnectionWithRetry() throws SQLException {
        for (int attempt = 1; ; attempt++) {
            try {
                return openCatalogConnectionOnce();
            } catch (SQLException exception) {
                if (attempt >= CATALOG_BOOTSTRAP_MAX_ATTEMPTS
                        || !isConcurrentCatalogBootstrapConflict(exception)) {
                    throw exception;
                }
                long backoff =
                        DuckLakeRetryBackoff.delayMillis(
                                CATALOG_BOOTSTRAP_INITIAL_BACKOFF_MILLIS, attempt - 1);
                try {
                    sleeper.sleep(backoff);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    SQLException failure =
                            new SQLException(
                                    "Interrupted while retrying DuckLake catalog initialization",
                                    interrupted);
                    failure.addSuppressed(exception);
                    throw failure;
                }
            }
        }
    }

    private String localCatalogConfigurationFingerprint() {
        return config.getStorageProvider().type()
                + '\0'
                + config.getStorageProvider().dataPath()
                + '\0'
                + config.getExtensionDirectory().orElse("")
                + '\0'
                + config.getExtensionRepository().orElse("")
                + '\0'
                + config.getDuckDbMemoryLimit()
                + '\0'
                + config.getDuckDbTempDirectory();
    }

    private Connection openCatalogConnectionOnce() throws SQLException {
        Connection connection = connectionProvider.open(JDBC_URL);
        try {
            configureExtensionLocations(connection);
            Set<DuckDbExtension> extensions = new LinkedHashSet<>();
            extensions.addAll(config.getStorageProvider().requiredExtensions());
            extensions.addAll(config.getCatalogProvider().requiredExtensions());
            extensions.add(DuckDbExtension.DUCKLAKE);
            installAndLoad(connection, extensions);
            config.getStorageProvider().configure(connection);
            config.getCatalogProvider().configure(connection);
            configureLocalResources(connection);
            execute(connection, attachStatement());
            return connection;
        } catch (SQLException | RuntimeException e) {
            closeAfterInitializationFailure(connection, e);
            throw e;
        }
    }

    private static boolean isConcurrentCatalogBootstrapConflict(SQLException exception) {
        for (SQLException current = exception;
                current != null;
                current = current.getNextException()) {
            String message = String.valueOf(current.getMessage()).toLowerCase(Locale.ROOT);
            if (message.contains("duplicate key")
                    || message.contains("already exists")
                    || message.contains("transaction conflict")
                    || message.contains("catalog conflict")) {
                return true;
            }
        }
        return false;
    }

    private void configureExtensionLocations(Connection connection) throws SQLException {
        if (config.getExtensionDirectory().isPresent()) {
            execute(
                    connection,
                    "SET extension_directory = "
                            + DuckDbSqlUtils.literal(config.getExtensionDirectory().get()));
        }
        if (config.getExtensionRepository().isPresent()) {
            execute(
                    connection,
                    "SET custom_extension_repository = "
                            + DuckDbSqlUtils.literal(config.getExtensionRepository().get()));
        }
    }

    private void installAndLoad(Connection connection, DuckDbExtension extension)
            throws SQLException {
        String extensionName = extension.extensionName();
        SQLException loadFailure;
        try {
            execute(connection, "LOAD " + extensionName);
            return;
        } catch (SQLException exception) {
            loadFailure = exception;
        }
        String install = "INSTALL " + extensionName;
        if (config.getExtensionRepository().isPresent()) {
            install += " FROM " + DuckDbSqlUtils.literal(config.getExtensionRepository().get());
        }
        try {
            execute(connection, install);
            execute(connection, "LOAD " + extensionName);
        } catch (SQLException installFailure) {
            installFailure.addSuppressed(loadFailure);
            throw installFailure;
        }
    }

    private void installAndLoad(Connection connection, Set<DuckDbExtension> extensions)
            throws SQLException {
        for (DuckDbExtension extension : extensions) {
            installAndLoad(connection, extension);
        }
    }

    private void configureLocalResources(Connection connection) throws SQLException {
        String tempDirectory =
                Paths.get(config.getDuckDbTempDirectory(), "duckdb-" + connectionIdSupplier.get())
                        .toString();
        try {
            Files.createDirectories(Paths.get(config.getDuckDbTempDirectory()));
        } catch (IOException e) {
            throw new SQLException("Unable to create DuckDB temporary directory", e);
        }
        execute(connection, "SET threads = 1");
        execute(
                connection,
                "SET memory_limit = " + DuckDbSqlUtils.literal(config.getDuckDbMemoryLimit()));
        execute(connection, "SET temp_directory = " + DuckDbSqlUtils.literal(tempDirectory));
        execute(connection, "SET preserve_insertion_order = false");
    }

    private String attachStatement() {
        return "ATTACH "
                + DuckDbSqlUtils.literal("ducklake:" + config.getCatalogProvider().metadataPath())
                + " AS "
                + DuckDbSqlUtils.quoteIdentifier(DUCKLAKE_CATALOG)
                + " (DATA_PATH "
                + DuckDbSqlUtils.literal(config.getStorageProvider().dataPath())
                + ", OVERRIDE_DATA_PATH false)";
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void closeAfterInitializationFailure(Connection connection, Exception failure) {
        try {
            connection.close();
        } catch (SQLException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    @Override
    public String toString() {
        return "DuckDbConnectionFactory{" + "config=" + config + '}';
    }

    @FunctionalInterface
    interface ConnectionProvider {
        Connection open(String url) throws SQLException;
    }
}
