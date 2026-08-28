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
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import java.time.Duration;

/** Configuration options for the DuckLake pipeline sink. */
@Internal
public final class DuckLakeDataSinkOptions {

    public static final String PROVIDER_TYPE_PROPERTY = "type";
    public static final String PREFIX_CATALOG_PROPERTIES = "catalog.properties.";

    public static final String PREFIX_STORAGE_PROPERTIES = "storage.properties.";

    public static final ConfigOption<String> CATALOG_TYPE =
            ConfigOptions.key(PREFIX_CATALOG_PROPERTIES + PROVIDER_TYPE_PROPERTY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Built-in DuckLake catalog provider type.");

    public static final ConfigOption<String> STORAGE_TYPE =
            ConfigOptions.key(PREFIX_STORAGE_PROPERTIES + PROVIDER_TYPE_PROPERTY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Built-in DuckLake storage provider type.");

    public static final ConfigOption<String> DUCKDB_EXTENSION_DIRECTORY =
            ConfigOptions.key("duckdb.extension-directory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional DuckDB extension directory.");

    public static final ConfigOption<String> DUCKDB_EXTENSION_REPOSITORY =
            ConfigOptions.key("duckdb.extension-repository")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional DuckDB extension repository.");

    public static final ConfigOption<String> DUCKDB_MEMORY_LIMIT =
            ConfigOptions.key("duckdb.memory-limit")
                    .stringType()
                    .defaultValue("512MB")
                    .withDescription("Memory limit of each embedded DuckDB connection.");

    public static final ConfigOption<String> DUCKDB_TEMP_DIRECTORY =
            ConfigOptions.key("duckdb.temp-directory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Local root directory used by embedded DuckDB connections for spilling.");

    public static final ConfigOption<Integer> SINK_COMMIT_MAX_RETRIES =
            ConfigOptions.key("sink.commit.max-retries")
                    .intType()
                    .defaultValue(8)
                    .withDescription("Maximum retries for retryable checkpoint commits.");

    public static final ConfigOption<Duration> SINK_COMMIT_RETRY_BACKOFF =
            ConfigOptions.key("sink.commit.retry-backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription("Initial retry backoff for checkpoint commits.");

    public static final ConfigOption<Duration> SINK_ORPHAN_CLEANUP_INTERVAL =
            ConfigOptions.key("sink.orphan-cleanup.interval")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            "Interval for best-effort cleanup of unregistered DuckLake files. "
                                    + "Set to zero to disable cleanup.");

    public static final ConfigOption<Duration> SINK_ORPHAN_RETENTION =
            ConfigOptions.key("sink.orphan-cleanup.retention")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription(
                            "Minimum age of unregistered files before cleanup. This must exceed "
                                    + "the maximum time a job may remain suspended before recovery.");

    public static final ConfigOption<Long> SINK_WRITER_MAX_EVENTS_PER_FILE =
            ConfigOptions.key("sink.writer.max-events-per-file")
                    .longType()
                    .defaultValue(100_000L)
                    .withDescription(
                            "Maximum CDC events buffered by one writer table before rolling files.");

    public static final ConfigOption<Long> SINK_WRITER_MAX_BUFFERED_EVENTS =
            ConfigOptions.key("sink.writer.max-buffered-events")
                    .longType()
                    .defaultValue(1_000_000L)
                    .withDescription(
                            "Maximum CDC events buffered across all tables in one sink writer.");

    public static final ConfigOption<Integer> SINK_WRITER_MAX_OPEN_TABLES =
            ConfigOptions.key("sink.writer.max-open-tables")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "Maximum number of table buffers kept open by one sink writer.");

    public static final ConfigOption<String> SINK_ID_PREFIX =
            ConfigOptions.key("sink.id-prefix")
                    .stringType()
                    .defaultValue("flink-cdc-")
                    .withDescription("Prefix used for the stable DuckLake writer job identifier.");

    public static String catalogProperty(String property) {
        return PREFIX_CATALOG_PROPERTIES + property;
    }

    public static String storageProperty(String property) {
        return PREFIX_STORAGE_PROPERTIES + property;
    }

    private DuckLakeDataSinkOptions() {}
}
