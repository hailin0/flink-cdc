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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DuckLakeDataChangeEventHashFunctionProviderTest {

    private static final TableId TABLE_ID = TableId.tableId("inventory", "binary_keys");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.VARBINARY(32).notNull())
                    .primaryKey("id")
                    .build();

    private final HashFunction<DataChangeEvent> hashFunction =
            new DuckLakeDataChangeEventHashFunctionProvider().getHashFunction(TABLE_ID, SCHEMA);

    @Test
    void hashesBinaryPrimaryKeysByContent() {
        byte[] firstKey = new byte[] {1, 2, 3, 4};
        byte[] secondKey = new byte[] {1, 2, 3, 4};

        assertThat(hashFunction.hashcode(insert(firstKey)))
                .isEqualTo(hashFunction.hashcode(insert(secondKey)));
    }

    @Test
    void routesAllOperationsForTheSamePrimaryKeyTogether() {
        RecordData before = record(new byte[] {1, 2, 3, 4});
        RecordData after = record(new byte[] {1, 2, 3, 4});

        assertThat(hashFunction.hashcode(DataChangeEvent.deleteEvent(TABLE_ID, before)))
                .isEqualTo(hashFunction.hashcode(DataChangeEvent.insertEvent(TABLE_ID, after)))
                .isEqualTo(
                        hashFunction.hashcode(
                                DataChangeEvent.updateEvent(TABLE_ID, before, after)));
    }

    private static DataChangeEvent insert(byte[] key) {
        return DataChangeEvent.insertEvent(TABLE_ID, record(key));
    }

    private static RecordData record(byte[] key) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(SCHEMA.getColumnDataTypes().toArray(new DataType[0]));
        return generator.generate(new Object[] {key});
    }
}
