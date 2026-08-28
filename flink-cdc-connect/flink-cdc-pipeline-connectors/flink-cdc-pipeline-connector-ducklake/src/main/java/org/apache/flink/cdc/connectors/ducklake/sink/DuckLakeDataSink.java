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
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.ducklake.sink.v2.DuckLakeSink;

import java.io.Serializable;

/**
 * High-throughput current-state mirror for CDC tables with stable, non-null primary keys.
 *
 * <p>Data changes are coalesced to the last row image per key. Idempotent Flink checkpoint commits
 * provide exactly-once current-state results. This sink intentionally does not preserve every
 * source change as DuckLake data-change-feed history or retain UPDATE row identity. Supported
 * schema changes are committed atomically with the surrounding data batches and checkpoint marker.
 */
@Internal
public final class DuckLakeDataSink implements DataSink, Serializable {

    private static final long serialVersionUID = 1L;

    private final DuckLakeSinkConfig config;

    DuckLakeDataSink(DuckLakeSinkConfig config) {
        this.config = config;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new DuckLakeSink(config));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new DuckLakeMetadataApplier();
    }

    @Override
    public HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider() {
        return new DuckLakeDataChangeEventHashFunctionProvider();
    }

    @Override
    public String toString() {
        return "DuckLakeDataSink{" + "config=" + config + '}';
    }
}
