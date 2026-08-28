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
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeHashUtils;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.event.SchemaChangeEventSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.DataTypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/** Versioned binary serializer for {@link DuckLakeCommittable}. */
@Internal
public final class DuckLakeCommittableSerializer
        implements SimpleVersionedSerializer<DuckLakeCommittable> {

    private static final int VERSION = 2;
    private static final int DATA_FILE = 0;
    private static final int SCHEMA_CHANGE = 1;
    private static final int MAX_COLLECTION_SIZE = 1_000_000;
    private static final int MAX_STRING_BYTES = 16 * 1024 * 1024;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DuckLakeCommittable committable) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(512);
        writeString(out, committable.getSinkId());
        writeString(out, committable.getOperatorId());
        if (committable.isSchemaChange()) {
            out.writeByte(SCHEMA_CHANGE);
            writeSchemaChangeResult(out, committable.getSchemaChangeResult());
        } else {
            out.writeByte(DATA_FILE);
            writeResult(out, committable.getWriteResult());
        }
        writeString(out, committable.getPayloadHash());
        return out.getCopyOfBuffer();
    }

    @Override
    public DuckLakeCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        if (version == 1) {
            return new DuckLakeCommittable(
                    readString(in), readString(in), readResult(in), readString(in));
        }
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        String sinkId = readString(in);
        String operatorId = readString(in);
        int kind = in.readUnsignedByte();
        if (kind == DATA_FILE) {
            return new DuckLakeCommittable(sinkId, operatorId, readResult(in), readString(in));
        }
        if (kind == SCHEMA_CHANGE) {
            return new DuckLakeCommittable(
                    sinkId, operatorId, readSchemaChangeResult(in), readString(in));
        }
        throw new IOException("Unknown DuckLake committable kind: " + kind);
    }

    static void writeResult(DataOutputView out, DuckLakeWriteResult writeResult)
            throws IOException {
        TableIdSerializer.INSTANCE.serialize(writeResult.getTableId(), out);
        out.writeLong(writeResult.getCheckpointId());
        out.writeInt(writeResult.getSchemaBatchIndex());
        out.writeInt(writeResult.getSubtaskId());
        out.writeInt(writeResult.getAttemptNumber());
        out.writeLong(writeResult.getFileSequence());

        List<DuckLakeColumnMetadata> columns = writeResult.getColumns();
        out.writeInt(columns.size());
        DataTypeSerializer dataTypeSerializer = new DataTypeSerializer();
        for (DuckLakeColumnMetadata column : columns) {
            writeString(out, column.getName());
            dataTypeSerializer.serialize(column.getDataType(), out);
        }

        writeStrings(out, writeResult.getPrimaryKeys());
        writeNullableString(out, writeResult.getDataFilePath());
        out.writeLong(writeResult.getDataRowCount());
        writeString(out, writeResult.getKeyFilePath());
        out.writeLong(writeResult.getKeyRowCount());
    }

    static DuckLakeWriteResult readResult(DataInputView in) throws IOException {
        TableId tableId = TableIdSerializer.INSTANCE.deserialize(in);
        long checkpointId = in.readLong();
        int schemaBatchIndex = in.readInt();
        int subtaskId = in.readInt();
        int attemptNumber = in.readInt();
        long fileSequence = in.readLong();

        int columnCount = readCollectionSize(in, "columns");
        List<DuckLakeColumnMetadata> columns = new ArrayList<>(columnCount);
        DataTypeSerializer dataTypeSerializer = new DataTypeSerializer();
        for (int i = 0; i < columnCount; i++) {
            String name = readString(in);
            DataType dataType = dataTypeSerializer.deserialize(in);
            columns.add(new DuckLakeColumnMetadata(name, dataType));
        }

        List<String> primaryKeys = readStrings(in, "primaryKeys");
        String dataFilePath = readNullableString(in);
        long dataRowCount = in.readLong();
        String keyFilePath = readString(in);
        long keyRowCount = in.readLong();
        return new DuckLakeWriteResult(
                tableId,
                checkpointId,
                schemaBatchIndex,
                subtaskId,
                attemptNumber,
                fileSequence,
                columns,
                primaryKeys,
                dataFilePath,
                dataRowCount,
                keyFilePath,
                keyRowCount);
    }

    static void writeSchemaChangeResult(
            DataOutputView out, DuckLakeSchemaChangeResult schemaChangeResult) throws IOException {
        out.writeLong(schemaChangeResult.getCheckpointId());
        out.writeInt(schemaChangeResult.getSchemaBatchIndex());
        out.writeInt(schemaChangeResult.getSubtaskId());
        out.writeInt(schemaChangeResult.getAttemptNumber());
        SchemaChangeEventSerializer.INSTANCE.serialize(schemaChangeResult.getEvent(), out);
    }

    static DuckLakeSchemaChangeResult readSchemaChangeResult(DataInputView in) throws IOException {
        return new DuckLakeSchemaChangeResult(
                in.readLong(),
                in.readInt(),
                in.readInt(),
                in.readInt(),
                SchemaChangeEventSerializer.INSTANCE.deserialize(in));
    }

    static String hashWriteResult(DuckLakeWriteResult writeResult) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(512);
        writeResult(out, writeResult);
        return hash(out);
    }

    static String hashSchemaChangeResult(DuckLakeSchemaChangeResult schemaChangeResult)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeLong(schemaChangeResult.getCheckpointId());
        out.writeInt(schemaChangeResult.getSchemaBatchIndex());
        SchemaChangeEventSerializer.INSTANCE.serialize(
                canonicalSchemaChange(schemaChangeResult.getEvent()), out);
        return hash(out);
    }

    private static SchemaChangeEvent canonicalSchemaChange(SchemaChangeEvent event) {
        if (event instanceof RenameColumnEvent) {
            RenameColumnEvent rename = (RenameColumnEvent) event;
            return new RenameColumnEvent(rename.tableId(), new TreeMap<>(rename.getNameMapping()));
        }
        if (event instanceof AlterColumnTypeEvent) {
            AlterColumnTypeEvent alter = (AlterColumnTypeEvent) event;
            return new AlterColumnTypeEvent(
                    alter.tableId(),
                    new TreeMap<>(alter.getTypeMapping()),
                    new TreeMap<>(alter.getOldTypeMapping()),
                    new TreeMap<>(alter.getComments()));
        }
        return event;
    }

    private static String hash(DataOutputSerializer out) throws IOException {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(out.getCopyOfBuffer());
            return DuckLakeHashUtils.toHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is not available", e);
        }
    }

    static void writeString(DataOutputView out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    static String readString(DataInputView in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > MAX_STRING_BYTES) {
            throw new IOException("Invalid string byte length: " + size);
        }
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static void writeNullableString(DataOutputView out, @Nullable String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeString(out, value);
        }
    }

    @Nullable
    static String readNullableString(DataInputView in) throws IOException {
        return in.readBoolean() ? readString(in) : null;
    }

    static void writeStrings(DataOutputView out, List<String> values) throws IOException {
        out.writeInt(values.size());
        for (String value : values) {
            writeString(out, value);
        }
    }

    static List<String> readStrings(DataInputView in, String fieldName) throws IOException {
        int size = readCollectionSize(in, fieldName);
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(readString(in));
        }
        return values;
    }

    static int readCollectionSize(DataInputView in, String fieldName) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > MAX_COLLECTION_SIZE) {
            throw new IOException("Invalid " + fieldName + " size: " + size);
        }
        return size;
    }
}
