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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Versioned binary serializer for {@link DuckLakeWriterState}. */
@Internal
public final class DuckLakeWriterStateSerializer
        implements SimpleVersionedSerializer<DuckLakeWriterState> {

    private static final int VERSION = 2;
    private static final int SCHEMA_SERIALIZER_VERSION = 2;
    private static final Comparator<TableId> TABLE_ID_COMPARATOR =
            Comparator.comparing(
                            TableId::getNamespace, Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparing(
                            TableId::getSchemaName,
                            Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparing(TableId::getTableName);

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DuckLakeWriterState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(1024);
        DuckLakeCommittableSerializer.writeString(out, state.getSinkId());
        DuckLakeCommittableSerializer.writeString(out, state.getOperatorId());
        out.writeLong(state.getWriterEpoch());
        writeSchemas(out, state.getCurrentSchemas());
        writeIntegerMap(out, state.getSchemaBatchIndexes());
        writeLongMap(out, state.getFileSequences());
        out.writeInt(state.getPendingWriteResults().size());
        for (DuckLakeWriteResult writeResult : state.getPendingWriteResults()) {
            DuckLakeCommittableSerializer.writeResult(out, writeResult);
        }
        out.writeInt(state.getPendingSchemaChangeResults().size());
        for (DuckLakeSchemaChangeResult schemaChangeResult :
                state.getPendingSchemaChangeResults()) {
            DuckLakeCommittableSerializer.writeSchemaChangeResult(out, schemaChangeResult);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public DuckLakeWriterState deserialize(int version, byte[] serialized) throws IOException {
        if (version != 1 && version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        String sinkId = DuckLakeCommittableSerializer.readString(in);
        String operatorId = DuckLakeCommittableSerializer.readString(in);
        long writerEpoch = in.readLong();
        Map<TableId, Schema> schemas = readSchemas(in);
        Map<TableId, Integer> batchIndexes = readIntegerMap(in, "schemaBatchIndexes");
        Map<TableId, Long> fileSequences = readLongMap(in, "fileSequences");
        int writeResultCount =
                DuckLakeCommittableSerializer.readCollectionSize(in, "pendingWriteResults");
        List<DuckLakeWriteResult> writeResults = new ArrayList<>(writeResultCount);
        for (int i = 0; i < writeResultCount; i++) {
            writeResults.add(DuckLakeCommittableSerializer.readResult(in));
        }
        List<DuckLakeSchemaChangeResult> schemaChangeResults = new ArrayList<>();
        if (version >= 2) {
            int schemaChangeCount =
                    DuckLakeCommittableSerializer.readCollectionSize(
                            in, "pendingSchemaChangeResults");
            for (int i = 0; i < schemaChangeCount; i++) {
                schemaChangeResults.add(DuckLakeCommittableSerializer.readSchemaChangeResult(in));
            }
        }
        return new DuckLakeWriterState(
                sinkId,
                operatorId,
                writerEpoch,
                schemas,
                batchIndexes,
                fileSequences,
                writeResults,
                schemaChangeResults);
    }

    private static void writeSchemas(DataOutputView out, Map<TableId, Schema> schemas)
            throws IOException {
        List<TableId> tableIds = sortedTableIds(schemas);
        out.writeInt(tableIds.size());
        for (TableId tableId : tableIds) {
            TableIdSerializer.INSTANCE.serialize(tableId, out);
            out.writeInt(SCHEMA_SERIALIZER_VERSION);
            SchemaSerializer.INSTANCE.serialize(schemas.get(tableId), out);
        }
    }

    private static Map<TableId, Schema> readSchemas(DataInputView in) throws IOException {
        int size = DuckLakeCommittableSerializer.readCollectionSize(in, "currentSchemas");
        Map<TableId, Schema> result = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            TableId tableId = TableIdSerializer.INSTANCE.deserialize(in);
            int schemaVersion = in.readInt();
            result.put(tableId, SchemaSerializer.INSTANCE.deserialize(schemaVersion, in));
        }
        return result;
    }

    private static void writeIntegerMap(DataOutputView out, Map<TableId, Integer> values)
            throws IOException {
        List<TableId> tableIds = sortedTableIds(values);
        out.writeInt(tableIds.size());
        for (TableId tableId : tableIds) {
            TableIdSerializer.INSTANCE.serialize(tableId, out);
            out.writeInt(values.get(tableId));
        }
    }

    private static Map<TableId, Integer> readIntegerMap(DataInputView in, String fieldName)
            throws IOException {
        int size = DuckLakeCommittableSerializer.readCollectionSize(in, fieldName);
        Map<TableId, Integer> result = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            result.put(TableIdSerializer.INSTANCE.deserialize(in), in.readInt());
        }
        return result;
    }

    private static void writeLongMap(DataOutputView out, Map<TableId, Long> values)
            throws IOException {
        List<TableId> tableIds = sortedTableIds(values);
        out.writeInt(tableIds.size());
        for (TableId tableId : tableIds) {
            TableIdSerializer.INSTANCE.serialize(tableId, out);
            out.writeLong(values.get(tableId));
        }
    }

    private static Map<TableId, Long> readLongMap(DataInputView in, String fieldName)
            throws IOException {
        int size = DuckLakeCommittableSerializer.readCollectionSize(in, fieldName);
        Map<TableId, Long> result = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            result.put(TableIdSerializer.INSTANCE.deserialize(in), in.readLong());
        }
        return result;
    }

    private static List<TableId> sortedTableIds(Map<TableId, ?> values) {
        List<TableId> tableIds = new ArrayList<>(values.keySet());
        tableIds.sort(TABLE_ID_COMPARATOR);
        return tableIds;
    }
}
