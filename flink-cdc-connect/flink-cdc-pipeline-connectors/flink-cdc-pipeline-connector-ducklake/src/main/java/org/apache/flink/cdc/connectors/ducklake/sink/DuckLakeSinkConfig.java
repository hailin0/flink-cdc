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
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.connectors.ducklake.sink.provider.DuckDbCatalogProviderFactory;
import org.apache.flink.cdc.connectors.ducklake.sink.provider.DuckLakeCatalogProvider;
import org.apache.flink.cdc.connectors.ducklake.sink.provider.DuckLakeProviderRegistry;
import org.apache.flink.cdc.connectors.ducklake.sink.provider.DuckLakeStorageProvider;
import org.apache.flink.cdc.connectors.ducklake.sink.provider.FileSystemStorageProviderFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Immutable validated DuckLake sink configuration. */
@Internal
public final class DuckLakeSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DuckLakeCatalogProvider catalogProvider;
    private final DuckLakeStorageProvider storageProvider;
    @Nullable private final String extensionDirectory;
    @Nullable private final String extensionRepository;
    private final String duckDbMemoryLimit;
    private final String duckDbTempDirectory;
    private final int commitMaxRetries;
    private final Duration commitRetryBackoff;
    private final Duration orphanCleanupInterval;
    private final Duration orphanRetention;
    private final long writerMaxEventsPerFile;
    private final long writerMaxBufferedEvents;
    private final int writerMaxOpenTables;
    private final String sinkIdPrefix;
    private final ZoneId pipelineZone;

    private DuckLakeSinkConfig(Configuration configuration, ZoneId pipelineZone) {
        Map<String, String> catalogProperties =
                properties(configuration, DuckLakeDataSinkOptions.PREFIX_CATALOG_PROPERTIES);
        Map<String, String> storageProperties =
                properties(configuration, DuckLakeDataSinkOptions.PREFIX_STORAGE_PROPERTIES);
        catalogProperties.remove(DuckLakeDataSinkOptions.PROVIDER_TYPE_PROPERTY);
        storageProperties.remove(DuckLakeDataSinkOptions.PROVIDER_TYPE_PROPERTY);
        this.catalogProvider =
                DuckLakeProviderRegistry.createCatalog(
                        configuration.get(DuckLakeDataSinkOptions.CATALOG_TYPE), catalogProperties);
        this.storageProvider =
                DuckLakeProviderRegistry.createStorage(
                        configuration.get(DuckLakeDataSinkOptions.STORAGE_TYPE), storageProperties);
        this.extensionDirectory =
                configuration
                        .getOptional(DuckLakeDataSinkOptions.DUCKDB_EXTENSION_DIRECTORY)
                        .orElse(null);
        this.extensionRepository =
                configuration
                        .getOptional(DuckLakeDataSinkOptions.DUCKDB_EXTENSION_REPOSITORY)
                        .orElse(null);
        this.duckDbMemoryLimit = configuration.get(DuckLakeDataSinkOptions.DUCKDB_MEMORY_LIMIT);
        this.duckDbTempDirectory =
                configuration
                        .getOptional(DuckLakeDataSinkOptions.DUCKDB_TEMP_DIRECTORY)
                        .orElseGet(
                                () ->
                                        Paths.get(
                                                        System.getProperty("java.io.tmpdir"),
                                                        "flink-cdc-ducklake")
                                                .toString());
        this.commitMaxRetries = configuration.get(DuckLakeDataSinkOptions.SINK_COMMIT_MAX_RETRIES);
        this.commitRetryBackoff =
                configuration.get(DuckLakeDataSinkOptions.SINK_COMMIT_RETRY_BACKOFF);
        this.orphanCleanupInterval =
                configuration.get(DuckLakeDataSinkOptions.SINK_ORPHAN_CLEANUP_INTERVAL);
        this.orphanRetention = configuration.get(DuckLakeDataSinkOptions.SINK_ORPHAN_RETENTION);
        this.writerMaxEventsPerFile =
                configuration.get(DuckLakeDataSinkOptions.SINK_WRITER_MAX_EVENTS_PER_FILE);
        this.writerMaxBufferedEvents =
                configuration.get(DuckLakeDataSinkOptions.SINK_WRITER_MAX_BUFFERED_EVENTS);
        this.writerMaxOpenTables =
                configuration.get(DuckLakeDataSinkOptions.SINK_WRITER_MAX_OPEN_TABLES);
        this.sinkIdPrefix = configuration.get(DuckLakeDataSinkOptions.SINK_ID_PREFIX);
        this.pipelineZone = pipelineZone;
        validate(storageProperties);
    }

    public static DuckLakeSinkConfig from(Configuration configuration) {
        return from(configuration, ZoneId.systemDefault());
    }

    public static DuckLakeSinkConfig from(Configuration configuration, ZoneId pipelineZone) {
        return new DuckLakeSinkConfig(configuration, pipelineZone);
    }

    public DuckLakeCatalogProvider getCatalogProvider() {
        return catalogProvider;
    }

    public DuckLakeStorageProvider getStorageProvider() {
        return storageProvider;
    }

    public String getStoragePath() {
        return storageProvider.dataPath();
    }

    public Optional<String> getExtensionDirectory() {
        return Optional.ofNullable(extensionDirectory);
    }

    public Optional<String> getExtensionRepository() {
        return Optional.ofNullable(extensionRepository);
    }

    public String getDuckDbMemoryLimit() {
        return duckDbMemoryLimit;
    }

    public String getDuckDbTempDirectory() {
        return duckDbTempDirectory;
    }

    public int getCommitMaxRetries() {
        return commitMaxRetries;
    }

    public Duration getCommitRetryBackoff() {
        return commitRetryBackoff;
    }

    public Duration getOrphanCleanupInterval() {
        return orphanCleanupInterval;
    }

    public Duration getOrphanRetention() {
        return orphanRetention;
    }

    public long getWriterMaxEventsPerFile() {
        return writerMaxEventsPerFile;
    }

    public long getWriterMaxBufferedEvents() {
        return writerMaxBufferedEvents;
    }

    public int getWriterMaxOpenTables() {
        return writerMaxOpenTables;
    }

    public String getSinkIdPrefix() {
        return sinkIdPrefix;
    }

    public ZoneId getPipelineZone() {
        return pipelineZone;
    }

    private static Map<String, String> properties(Configuration configuration, String prefix) {
        Map<String, String> result = new LinkedHashMap<>();
        configuration
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(prefix)) {
                                result.put(key.substring(prefix.length()), value);
                            }
                        });
        return result;
    }

    private void validate(Map<String, String> storageProperties) {
        if (DuckDbCatalogProviderFactory.TYPE.equals(catalogProvider.type())
                && !FileSystemStorageProviderFactory.TYPE.equals(storageProvider.type())) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.CATALOG_TYPE.key()
                            + "=duckdb is supported only with "
                            + DuckLakeDataSinkOptions.STORAGE_TYPE.key()
                            + "=filesystem");
        }
        if (FileSystemStorageProviderFactory.TYPE.equals(storageProvider.type())
                && !DuckDbCatalogProviderFactory.TYPE.equals(catalogProvider.type())
                && !Boolean.parseBoolean(
                        storageProperties.getOrDefault(
                                FileSystemStorageProviderFactory.SHARED_PROPERTY, "false"))) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.storageProperty(
                                    FileSystemStorageProviderFactory.SHARED_PROPERTY)
                            + " must be true with a non-DuckDB catalog");
        }
        if (duckDbMemoryLimit.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.DUCKDB_MEMORY_LIMIT.key() + " must not be blank");
        }
        if (duckDbTempDirectory.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.DUCKDB_TEMP_DIRECTORY.key() + " must not be blank");
        }
        if (commitMaxRetries < 0) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.SINK_COMMIT_MAX_RETRIES.key()
                            + " must not be negative");
        }
        if (commitRetryBackoff.isNegative()) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.SINK_COMMIT_RETRY_BACKOFF.key()
                            + " must not be negative");
        }
        if (orphanCleanupInterval.isNegative()) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.SINK_ORPHAN_CLEANUP_INTERVAL.key()
                            + " must not be negative");
        }
        if (orphanRetention.compareTo(Duration.ofSeconds(1)) < 0) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.SINK_ORPHAN_RETENTION.key() + " must be positive");
        }
        if (writerMaxEventsPerFile <= 0) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.SINK_WRITER_MAX_EVENTS_PER_FILE.key()
                            + " must be positive");
        }
        if (writerMaxBufferedEvents <= 0) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.SINK_WRITER_MAX_BUFFERED_EVENTS.key()
                            + " must be positive");
        }
        if (writerMaxOpenTables <= 0) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.SINK_WRITER_MAX_OPEN_TABLES.key()
                            + " must be positive");
        }
    }

    @Override
    public String toString() {
        return "DuckLakeSinkConfig{"
                + "catalogProvider="
                + catalogProvider
                + ", storageProvider="
                + storageProvider
                + ", extensionDirectory="
                + getExtensionDirectory()
                + ", extensionRepository="
                + (extensionRepository == null ? "<default>" : "<configured>")
                + ", duckDbMemoryLimit='"
                + duckDbMemoryLimit
                + '\''
                + ", duckDbTempDirectory='"
                + duckDbTempDirectory
                + '\''
                + ", commitMaxRetries="
                + commitMaxRetries
                + ", commitRetryBackoff="
                + commitRetryBackoff
                + ", orphanCleanupInterval="
                + orphanCleanupInterval
                + ", orphanRetention="
                + orphanRetention
                + ", writerMaxEventsPerFile="
                + writerMaxEventsPerFile
                + ", writerMaxBufferedEvents="
                + writerMaxBufferedEvents
                + ", writerMaxOpenTables="
                + writerMaxOpenTables
                + ", sinkIdPrefix='"
                + sinkIdPrefix
                + '\''
                + ", pipelineZone="
                + pipelineZone
                + '}';
    }
}
