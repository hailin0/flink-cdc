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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeWriterStateSerializerTest {

    private static final TableId ORDERS = TableId.tableId("sales", "orders");
    private static final TableId CUSTOMERS = TableId.tableId("crm", "customer.detail");

    private final DuckLakeWriterStateSerializer serializer = new DuckLakeWriterStateSerializer();

    @Test
    void roundTripsEveryField() throws Exception {
        DuckLakeWriterState state = createState(false);

        byte[] serialized = serializer.serialize(state);

        assertThat(serializer.deserialize(serializer.getVersion(), serialized)).isEqualTo(state);
    }

    @Test
    void producesDeterministicBytesRegardlessOfMapInsertionOrder() throws Exception {
        DuckLakeWriterState first = createState(false);
        DuckLakeWriterState reversed = createState(true);

        assertThat(serializer.serialize(first)).isEqualTo(serializer.serialize(reversed));
        assertThat(serializer.serialize(first)).isEqualTo(serializer.serialize(first));
    }

    @Test
    void rejectsUnknownVersion() throws Exception {
        byte[] serialized = serializer.serialize(createState(false));

        assertThatThrownBy(() -> serializer.deserialize(999, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version: 999");
    }

    private static DuckLakeWriterState createState(boolean reverseInsertionOrder) {
        Map<TableId, Schema> schemas = new HashMap<>();
        Map<TableId, Integer> batches = new HashMap<>();
        Map<TableId, Long> sequences = new HashMap<>();
        List<TableId> order =
                reverseInsertionOrder
                        ? Arrays.asList(CUSTOMERS, ORDERS)
                        : Arrays.asList(ORDERS, CUSTOMERS);
        for (TableId tableId : order) {
            if (tableId.equals(ORDERS)) {
                schemas.put(tableId, DuckLakeTestData.ordersSchema());
                batches.put(tableId, 3);
                sequences.put(tableId, 12L);
            } else {
                schemas.put(tableId, DuckLakeTestData.customerSchema());
                batches.put(tableId, 1);
                sequences.put(tableId, 5L);
            }
        }
        List<DuckLakeWriteResult> writeResults =
                Arrays.asList(
                        DuckLakeTestData.committable(null, 0).getWriteResult(),
                        DuckLakeTestData.committable("s3://bucket/pending/data.parquet", 8)
                                .getWriteResult());
        return new DuckLakeWriterState(
                "sink-instance-1", "operator-9", 43, schemas, batches, sequences, writeResults);
    }
}
