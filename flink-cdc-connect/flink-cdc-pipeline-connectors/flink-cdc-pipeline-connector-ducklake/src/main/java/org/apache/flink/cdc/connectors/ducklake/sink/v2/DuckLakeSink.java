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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.ducklake.sink.DuckLakeSinkConfig;
import org.apache.flink.cdc.connectors.ducklake.sink.client.DuckDbConnectionFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Flink Sink V2 entry point for parallel DuckLake writers and globally partitioned commits. */
@Internal
public final class DuckLakeSink
        implements Sink<Event>,
                TwoPhaseCommittingSink<Event, DuckLakeCommittable>,
                SupportsWriterState<Event, DuckLakeWriterState>,
                WithPreCommitTopology<Event, DuckLakeCommittable> {

    private final DuckLakeSinkConfig config;
    private final String sinkId;
    private final String operatorId;

    public DuckLakeSink(DuckLakeSinkConfig config) {
        this.config = config;
        this.sinkId = config.getSinkIdPrefix() + UUID.randomUUID();
        this.operatorId = UUID.randomUUID().toString();
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) {
        return create(context, null);
    }

    @Deprecated
    @Override
    public SinkWriter<Event> createWriter(Sink.InitContext context) {
        return create(context, null);
    }

    @Override
    public StatefulSinkWriter<Event, DuckLakeWriterState> restoreWriter(
            WriterInitContext context, Collection<DuckLakeWriterState> states) {
        DuckLakeWriterState state = mergeStates(states);
        if (state != null) {
            state = rebindState(state);
        }
        return create(context, state);
    }

    private DuckLakeWriterState rebindState(DuckLakeWriterState state) {
        return new DuckLakeWriterState(
                sinkId,
                operatorId,
                state.getWriterEpoch(),
                state.getCurrentSchemas(),
                state.getSchemaBatchIndexes(),
                state.getFileSequences(),
                state.getPendingWriteResults(),
                state.getPendingSchemaChangeResults());
    }

    private DuckLakeWriterState mergeStates(Collection<DuckLakeWriterState> states) {
        if (states == null || states.isEmpty()) {
            return null;
        }
        DuckLakeWriterState first = states.iterator().next();
        Map<TableId, Schema> schemas = new HashMap<>();
        Map<TableId, Integer> batches = new HashMap<>();
        Map<TableId, Long> sequences = new HashMap<>();
        ArrayList<DuckLakeWriteResult> writeResults = new ArrayList<>();
        ArrayList<DuckLakeSchemaChangeResult> schemaChangeResults = new ArrayList<>();
        long writerEpoch = first.getWriterEpoch();
        for (DuckLakeWriterState state : states) {
            if (!first.getSinkId().equals(state.getSinkId())
                    || !first.getOperatorId().equals(state.getOperatorId())) {
                throw new IllegalArgumentException("Incompatible DuckLake writer states");
            }
            writerEpoch = Math.max(writerEpoch, state.getWriterEpoch());
            state.getCurrentSchemas()
                    .forEach(
                            (table, schema) -> {
                                Schema previous = schemas.putIfAbsent(table, schema);
                                if (previous != null && !previous.equals(schema)) {
                                    throw new IllegalArgumentException(
                                            "Conflicting restored schema for " + table);
                                }
                            });
            state.getSchemaBatchIndexes()
                    .forEach(
                            (table, batch) -> {
                                Integer previous = batches.putIfAbsent(table, batch);
                                if (previous != null && !previous.equals(batch)) {
                                    throw new IllegalArgumentException(
                                            "Conflicting restored schema batch for " + table);
                                }
                            });
            state.getFileSequences()
                    .forEach((table, sequence) -> sequences.merge(table, sequence, Math::max));
            writeResults.addAll(state.getPendingWriteResults());
            schemaChangeResults.addAll(state.getPendingSchemaChangeResults());
        }
        return new DuckLakeWriterState(
                first.getSinkId(),
                first.getOperatorId(),
                writerEpoch,
                schemas,
                batches,
                sequences,
                writeResults,
                schemaChangeResults);
    }

    private DuckLakeWriter create(
            org.apache.flink.api.connector.sink2.InitContext context, DuckLakeWriterState state) {
        long initialWriterEpoch =
                state == null
                        ? context.getRestoredCheckpointId()
                                        .orElse(
                                                org.apache.flink.api.connector.sink2.InitContext
                                                                .INITIAL_CHECKPOINT_ID
                                                        - 1)
                                + 1
                        : state.getWriterEpoch();
        DuckDbConnectionFactory factory = new DuckDbConnectionFactory(config);
        return new DuckLakeWriter(
                factory::openWriterConnection,
                new DuckLakeCatalogSchemaProvider(factory),
                config.getStoragePath(),
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getTaskInfo().getAttemptNumber(),
                config.getPipelineZone(),
                initialWriterEpoch,
                sinkId,
                operatorId,
                state,
                config.getWriterMaxEventsPerFile(),
                config.getWriterMaxBufferedEvents(),
                config.getWriterMaxOpenTables());
    }

    @Override
    public Committer<DuckLakeCommittable> createCommitter() {
        return createCommitter(null);
    }

    @Override
    public Committer<DuckLakeCommittable> createCommitter(CommitterInitContext context) {
        return new DuckLakeCommitter(
                new DuckLakeJdbcCommitExecutor(new DuckDbConnectionFactory(config)),
                config.getCommitMaxRetries(),
                config.getCommitRetryBackoff(),
                config.getOrphanCleanupInterval(),
                config.getOrphanRetention(),
                System::currentTimeMillis,
                context == null ? null : context.metricGroup());
    }

    @Override
    public SimpleVersionedSerializer<DuckLakeCommittable> getCommittableSerializer() {
        return new DuckLakeCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DuckLakeCommittable> getWriteResultSerializer() {
        return getCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DuckLakeWriterState> getWriterStateSerializer() {
        return new DuckLakeWriterStateSerializer();
    }

    @Override
    public DataStream<CommittableMessage<DuckLakeCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<DuckLakeCommittable>> committables) {
        return committables
                .map(
                        (MapFunction<
                                        CommittableMessage<DuckLakeCommittable>,
                                        CommittableMessage<DuckLakeCommittable>>)
                                DuckLakeSink::normalizeCheckpointLineage)
                .returns(CommittableMessageTypeInfo.of(this::getCommittableSerializer))
                .name("DuckLake checkpoint lineage")
                .global();
    }

    static CommittableMessage<DuckLakeCommittable> normalizeCheckpointLineage(
            CommittableMessage<DuckLakeCommittable> message) throws IOException {
        if (!(message instanceof CommittableWithLineage)) {
            return message;
        }
        CommittableWithLineage<DuckLakeCommittable> withLineage =
                (CommittableWithLineage<DuckLakeCommittable>) message;
        DuckLakeCommittable normalized =
                withLineage.getCommittable().withCheckpointId(message.getCheckpointIdOrEOI());
        return withLineage.map(ignored -> normalized);
    }
}
