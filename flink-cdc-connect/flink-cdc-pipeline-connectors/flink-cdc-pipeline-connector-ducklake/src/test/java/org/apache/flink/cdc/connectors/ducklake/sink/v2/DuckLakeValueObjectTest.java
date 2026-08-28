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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeValueObjectTest {

    private static final TableId ORDERS = TableId.tableId("sales", "orders");

    @Test
    void writeResultDefensivelyCopiesCollections() {
        List<DuckLakeColumnMetadata> columns = new ArrayList<>(DuckLakeTestData.supportedColumns());
        List<String> primaryKeys = new ArrayList<>(Arrays.asList("tenant\"id", "id"));
        DuckLakeWriteResult writeResult =
                new DuckLakeWriteResult(
                        TableId.tableId("sales\"schema", "order.detail"),
                        42,
                        3,
                        7,
                        2,
                        11,
                        columns,
                        primaryKeys,
                        null,
                        0,
                        "s3://bucket/pending/keys.parquet",
                        9);

        columns.clear();
        primaryKeys.clear();

        assertThat(writeResult.getColumns()).hasSize(DuckLakeTestData.supportedColumns().size());
        assertThat(writeResult.getPrimaryKeys()).containsExactly("tenant\"id", "id");
        assertThatThrownBy(() -> writeResult.getColumns().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void writerStateDefensivelyCopiesCollections() {
        Map<TableId, Schema> schemas = new HashMap<>();
        schemas.put(ORDERS, DuckLakeTestData.ordersSchema());
        Map<TableId, Integer> batches = new HashMap<>();
        batches.put(ORDERS, 2);
        Map<TableId, Long> sequences = new HashMap<>();
        sequences.put(ORDERS, 4L);
        List<DuckLakeWriteResult> writeResults = new ArrayList<>();
        writeResults.add(DuckLakeTestData.committable(null, 0).getWriteResult());
        DuckLakeWriterState state =
                new DuckLakeWriterState(
                        "sink-instance-1",
                        "operator-9",
                        43,
                        schemas,
                        batches,
                        sequences,
                        writeResults);

        schemas.clear();
        batches.clear();
        sequences.clear();
        writeResults.clear();

        assertThat(state.getCurrentSchemas()).containsKey(ORDERS);
        assertThat(state.getSchemaBatchIndexes()).containsEntry(ORDERS, 2);
        assertThat(state.getFileSequences()).containsEntry(ORDERS, 4L);
        assertThat(state.getPendingWriteResults()).hasSize(1);
        assertThatThrownBy(() -> state.getCurrentSchemas().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
