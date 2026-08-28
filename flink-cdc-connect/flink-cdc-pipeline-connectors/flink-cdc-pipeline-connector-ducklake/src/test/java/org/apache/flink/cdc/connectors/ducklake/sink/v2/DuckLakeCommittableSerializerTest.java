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

import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeCommittableSerializerTest {

    private final DuckLakeCommittableSerializer serializer = new DuckLakeCommittableSerializer();

    @Test
    void roundTripsEveryFieldAndSupportedType() throws Exception {
        DuckLakeCommittable committable = DuckLakeTestData.committable(null, 0);

        byte[] serialized = serializer.serialize(committable);

        assertThat(serializer.deserialize(serializer.getVersion(), serialized))
                .isEqualTo(committable);
    }

    @Test
    void roundTripsSchemaChange() throws Exception {
        DuckLakeCommittable committable = DuckLakeTestData.schemaChangeResult("orders", 1, 3);

        byte[] serialized = serializer.serialize(committable);

        assertThat(serializer.deserialize(serializer.getVersion(), serialized))
                .isEqualTo(committable);
    }

    @Test
    void schemaChangeHashDoesNotDependOnBroadcastWriter() throws Exception {
        DuckLakeCommittable writer0 = DuckLakeTestData.schemaChangeResult("orders", 1, 0);
        DuckLakeCommittable writer1 = DuckLakeTestData.schemaChangeResult("orders", 1, 1);

        assertThat(writer0.getPayloadHash()).isEqualTo(writer1.getPayloadHash());
    }

    @Test
    void schemaChangeHashDoesNotDependOnMapInsertionOrder() throws Exception {
        Map<String, String> firstMapping = new LinkedHashMap<>();
        firstMapping.put("first", "renamed_first");
        firstMapping.put("second", "renamed_second");
        Map<String, String> reversedMapping = new LinkedHashMap<>();
        reversedMapping.put("second", "renamed_second");
        reversedMapping.put("first", "renamed_first");
        DuckLakeSchemaChangeResult first = schemaChangeResult(firstMapping);
        DuckLakeSchemaChangeResult reversed = schemaChangeResult(reversedMapping);

        assertThat(DuckLakeCommittableSerializer.hashSchemaChangeResult(first))
                .isEqualTo(DuckLakeCommittableSerializer.hashSchemaChangeResult(reversed));
    }

    @Test
    void rejectsUnknownVersion() throws Exception {
        byte[] serialized = serializer.serialize(DuckLakeTestData.committable(null, 0));

        assertThatThrownBy(() -> serializer.deserialize(999, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version: 999");
    }

    @Test
    void readsVersionOneDataCommittable() throws Exception {
        DuckLakeCommittable expected = DuckLakeTestData.committable(null, 0);
        DataOutputSerializer out = new DataOutputSerializer(512);
        DuckLakeCommittableSerializer.writeString(out, expected.getSinkId());
        DuckLakeCommittableSerializer.writeString(out, expected.getOperatorId());
        DuckLakeCommittableSerializer.writeResult(out, expected.getWriteResult());
        DuckLakeCommittableSerializer.writeString(out, expected.getPayloadHash());

        assertThat(serializer.deserialize(1, out.getCopyOfBuffer())).isEqualTo(expected);
    }

    private static DuckLakeSchemaChangeResult schemaChangeResult(Map<String, String> mapping) {
        return new DuckLakeSchemaChangeResult(
                8, 1, 0, 0, new RenameColumnEvent(TableId.tableId("sales", "orders"), mapping));
    }
}
