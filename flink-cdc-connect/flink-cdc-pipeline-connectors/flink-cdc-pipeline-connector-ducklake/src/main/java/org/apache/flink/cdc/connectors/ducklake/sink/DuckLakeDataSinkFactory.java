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
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.table.api.ValidationException;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** A {@link DataSinkFactory} for DuckLake. */
@Internal
public final class DuckLakeDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "ducklake";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(
                        DuckLakeDataSinkOptions.PREFIX_CATALOG_PROPERTIES,
                        DuckLakeDataSinkOptions.PREFIX_STORAGE_PROPERTIES);
        try {
            return new DuckLakeDataSink(
                    DuckLakeSinkConfig.from(
                            context.getFactoryConfiguration(), pipelineZone(context)));
        } catch (IllegalArgumentException e) {
            throw new ValidationException(e.getMessage(), e);
        }
    }

    private static ZoneId pipelineZone(Context context) {
        String configuredZone =
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE);
        if (PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(configuredZone)) {
            return ZoneId.systemDefault();
        }
        return ZoneId.of(configuredZone);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(
                        DuckLakeDataSinkOptions.CATALOG_TYPE,
                        DuckLakeDataSinkOptions.STORAGE_TYPE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        DuckLakeDataSinkOptions.DUCKDB_EXTENSION_DIRECTORY,
                        DuckLakeDataSinkOptions.DUCKDB_EXTENSION_REPOSITORY,
                        DuckLakeDataSinkOptions.DUCKDB_MEMORY_LIMIT,
                        DuckLakeDataSinkOptions.DUCKDB_TEMP_DIRECTORY,
                        DuckLakeDataSinkOptions.SINK_COMMIT_MAX_RETRIES,
                        DuckLakeDataSinkOptions.SINK_COMMIT_RETRY_BACKOFF,
                        DuckLakeDataSinkOptions.SINK_ORPHAN_CLEANUP_INTERVAL,
                        DuckLakeDataSinkOptions.SINK_ORPHAN_RETENTION,
                        DuckLakeDataSinkOptions.SINK_WRITER_MAX_EVENTS_PER_FILE,
                        DuckLakeDataSinkOptions.SINK_WRITER_MAX_BUFFERED_EVENTS,
                        DuckLakeDataSinkOptions.SINK_WRITER_MAX_OPEN_TABLES,
                        DuckLakeDataSinkOptions.SINK_ID_PREFIX));
    }
}
