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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.ducklake.sink.DuckLakeSinkConfig;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class DuckLakeSinkTest {

    @Test
    void providesSerializersAndCommitterWithoutConnecting() throws Exception {
        DuckLakeSink sink = new DuckLakeSink(config());

        assertThat(sink.getCommittableSerializer())
                .isInstanceOf(DuckLakeCommittableSerializer.class);
        assertThat(sink.getWriteResultSerializer())
                .isInstanceOf(DuckLakeCommittableSerializer.class);
        assertThat(sink.getWriterStateSerializer())
                .isInstanceOf(DuckLakeWriterStateSerializer.class);
        Committer<DuckLakeCommittable> committer = sink.createCommitter();
        assertThat(committer).isInstanceOf(DuckLakeCommitter.class);
        committer.close();
    }

    @Test
    void isSerializableForFlinkJobGraph() throws Exception {
        DuckLakeSink sink = new DuckLakeSink(config());

        Object restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(sink), getClass().getClassLoader());

        assertThat(restored).isInstanceOf(DuckLakeSink.class);
    }

    @Test
    void rebindsRestoredAndNewWritersToOneSavepointJobIdentity() throws Exception {
        DuckLakeWriterState oldState =
                new DuckLakeWriterState(
                        "old-sink",
                        "old-operator",
                        6,
                        Collections.singletonMap(
                                TableId.tableId("sales", "orders"),
                                Schema.newBuilder()
                                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                                        .primaryKey("id")
                                        .build()),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList());
        DuckLakeSink sink = new DuckLakeSink(config());

        StatefulSinkWriter<Event, DuckLakeWriterState> restored =
                sink.restoreWriter(context(0, 2, 5), Collections.singletonList(oldState));
        StatefulSinkWriter<Event, DuckLakeWriterState> scaledOut =
                sink.restoreWriter(context(1, 2, 5), Collections.emptyList());

        DuckLakeWriterState restoredSnapshot = restored.snapshotState(6).get(0);
        DuckLakeWriterState scaledOutSnapshot = scaledOut.snapshotState(6).get(0);
        assertThat(restoredSnapshot.getSinkId())
                .isEqualTo(scaledOutSnapshot.getSinkId())
                .isNotEqualTo("old-sink");
        assertThat(restoredSnapshot.getOperatorId())
                .isEqualTo(scaledOutSnapshot.getOperatorId())
                .isNotEqualTo("old-operator");
        restored.close();
        scaledOut.close();
    }

    @Test
    void replacesWriterEpochWithCheckpointLineage() throws Exception {
        DuckLakeCommittable provisional =
                DuckLakeTestData.committable("orders", 0, 0, 0, "provisional");
        CommittableWithLineage<DuckLakeCommittable> message =
                new CommittableWithLineage<>(provisional, 37, 0);

        CommittableMessage<DuckLakeCommittable> normalized =
                DuckLakeSink.normalizeCheckpointLineage(message);

        assertThat(normalized).isInstanceOf(CommittableWithLineage.class);
        CommittableWithLineage<DuckLakeCommittable> normalizedWithLineage =
                (CommittableWithLineage<DuckLakeCommittable>) normalized;
        assertThat(normalizedWithLineage.getCheckpointIdOrEOI()).isEqualTo(37);
        assertThat(normalizedWithLineage.getCommittable().getCheckpointId()).isEqualTo(37);
        assertThat(normalizedWithLineage.getCommittable().getWriteResultHash())
                .isNotEqualTo(provisional.getWriteResultHash());
    }

    @Test
    void mergesWriterStatesWithDivergedLocalEpochsAfterAbortedCheckpoint() throws Exception {
        DuckLakeWriterState first = emptyState("old-sink", "old-operator", 4);
        DuckLakeWriterState second = emptyState("old-sink", "old-operator", 5);
        DuckLakeSink sink = new DuckLakeSink(config());

        StatefulSinkWriter<Event, DuckLakeWriterState> restored =
                sink.restoreWriter(context(0, 1, 3), java.util.Arrays.asList(first, second));

        assertThat(restored.snapshotState(6).get(0).getWriterEpoch()).isEqualTo(5);
        restored.close();
    }

    private static DuckLakeWriterState emptyState(
            String sinkId, String operatorId, long writerEpoch) {
        return new DuckLakeWriterState(
                sinkId,
                operatorId,
                writerEpoch,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private static WriterInitContext context(
            int subtaskId, int parallelism, long restoredCheckpointId) {
        TaskInfo taskInfo =
                (TaskInfo)
                        Proxy.newProxyInstance(
                                DuckLakeSinkTest.class.getClassLoader(),
                                new Class<?>[] {TaskInfo.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "getIndexOfThisSubtask":
                                            return subtaskId;
                                        case "getNumberOfParallelSubtasks":
                                        case "getMaxNumberOfParallelSubtasks":
                                            return parallelism;
                                        case "getAttemptNumber":
                                            return 0;
                                        default:
                                            return null;
                                    }
                                });
        return (WriterInitContext)
                Proxy.newProxyInstance(
                        DuckLakeSinkTest.class.getClassLoader(),
                        new Class<?>[] {WriterInitContext.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getTaskInfo")) {
                                return taskInfo;
                            }
                            if (method.getName().equals("getRestoredCheckpointId")) {
                                return OptionalLong.of(restoredCheckpointId);
                            }
                            return null;
                        });
    }

    private static DuckLakeSinkConfig config() {
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
        return DuckLakeSinkConfig.from(Configuration.fromMap(options), ZoneId.of("UTC"));
    }
}
