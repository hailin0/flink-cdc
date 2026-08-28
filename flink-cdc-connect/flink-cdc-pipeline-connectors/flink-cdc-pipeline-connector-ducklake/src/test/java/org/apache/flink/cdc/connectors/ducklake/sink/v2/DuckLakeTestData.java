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
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Canonical domain objects shared by serializer and value-object tests. */
final class DuckLakeTestData {

    private DuckLakeTestData() {}

    static DuckLakeCommittable committable(String dataPath, long dataRowCount) {
        DuckLakeWriteResult writeResult =
                new DuckLakeWriteResult(
                        TableId.tableId("sales\"schema", "order.detail"),
                        42,
                        3,
                        7,
                        2,
                        11,
                        supportedColumns(),
                        Arrays.asList("tenant\"id", "id"),
                        dataPath,
                        dataRowCount,
                        "s3://bucket/pending/keys-'quoted'.parquet",
                        17);
        return new DuckLakeCommittable(
                "sink-instance-1", "operator-9", writeResult, "sha256:0123456789abcdef");
    }

    static DuckLakeCommittable committable(
            String table, int batch, int writer, long sequence, String hash) {
        DuckLakeWriteResult writeResult =
                new DuckLakeWriteResult(
                        TableId.tableId("schema", table),
                        8,
                        batch,
                        writer,
                        0,
                        sequence,
                        Collections.singletonList(
                                new DuckLakeColumnMetadata("id", DataTypes.BIGINT().notNull())),
                        Collections.singletonList("id"),
                        "s3://bucket/data/" + hash + ".parquet",
                        1,
                        "s3://bucket/keys/" + hash + ".parquet",
                        1);
        return new DuckLakeCommittable("sink-1", "operator-1", writeResult, hash);
    }

    static DuckLakeCommittable schemaChangeResult(String table, int batch, int writer)
            throws Exception {
        DuckLakeSchemaChangeResult change =
                new DuckLakeSchemaChangeResult(
                        8,
                        batch,
                        writer,
                        0,
                        new RenameColumnEvent(
                                TableId.tableId("schema", table),
                                Collections.singletonMap("old_name", "new_name")));
        return new DuckLakeCommittable(
                "sink-1",
                "operator-1",
                change,
                DuckLakeCommittableSerializer.hashSchemaChangeResult(change));
    }

    static List<DuckLakeColumnMetadata> supportedColumns() {
        return Arrays.asList(
                new DuckLakeColumnMetadata("tenant\"id", DataTypes.STRING().notNull()),
                new DuckLakeColumnMetadata("id", DataTypes.BIGINT().notNull()),
                new DuckLakeColumnMetadata("bool", DataTypes.BOOLEAN()),
                new DuckLakeColumnMetadata("tiny", DataTypes.TINYINT()),
                new DuckLakeColumnMetadata("small", DataTypes.SMALLINT()),
                new DuckLakeColumnMetadata("integer", DataTypes.INT()),
                new DuckLakeColumnMetadata("float", DataTypes.FLOAT()),
                new DuckLakeColumnMetadata("double", DataTypes.DOUBLE()),
                new DuckLakeColumnMetadata("decimal", DataTypes.DECIMAL(20, 4)),
                new DuckLakeColumnMetadata("char", DataTypes.CHAR(12)),
                new DuckLakeColumnMetadata("binary", DataTypes.BINARY(16)),
                new DuckLakeColumnMetadata("bytes", DataTypes.BYTES()),
                new DuckLakeColumnMetadata("date", DataTypes.DATE()),
                new DuckLakeColumnMetadata("time", DataTypes.TIME(6)),
                new DuckLakeColumnMetadata("timestamp", DataTypes.TIMESTAMP(9)),
                new DuckLakeColumnMetadata("timestamp_ltz", DataTypes.TIMESTAMP_LTZ(6)));
    }

    static Schema ordersSchema() {
        return Schema.newBuilder()
                .physicalColumn("tenant\"id", DataTypes.STRING().notNull())
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("amount", DataTypes.DECIMAL(20, 4))
                .primaryKey("tenant\"id", "id")
                .comment("orders schema")
                .build();
    }

    static Schema customerSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT().notNull())
                .physicalColumn("updated_at", DataTypes.TIMESTAMP_LTZ(6))
                .primaryKey("id")
                .build();
    }
}
