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
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Content-based primary-key hash function used by parallel DuckLake writers. */
@Internal
final class DuckLakeDataChangeEventHashFunctionProvider
        implements HashFunctionProvider<DataChangeEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public HashFunction<DataChangeEvent> getHashFunction(@Nullable TableId tableId, Schema schema) {
        List<RecordData.FieldGetter> primaryKeyGetters = new ArrayList<>();
        for (String primaryKey : schema.primaryKeys()) {
            int position = schema.getColumnNames().indexOf(primaryKey);
            if (position < 0) {
                throw new IllegalStateException(
                        "Unable to find primary-key column in DuckLake schema: " + primaryKey);
            }
            primaryKeyGetters.add(
                    RecordData.createFieldGetter(
                            schema.getColumns().get(position).getType(), position));
        }
        return event -> hash(event, primaryKeyGetters);
    }

    private static int hash(DataChangeEvent event, List<RecordData.FieldGetter> primaryKeyGetters) {
        List<Object> values = new ArrayList<>(primaryKeyGetters.size() + 3);
        TableId eventTable = event.tableId();
        if (eventTable.getNamespace() != null) {
            values.add(eventTable.getNamespace());
        }
        if (eventTable.getSchemaName() != null) {
            values.add(eventTable.getSchemaName());
        }
        values.add(eventTable.getTableName());
        RecordData record =
                Objects.requireNonNull(
                        event.op() == OperationType.DELETE ? event.before() : event.after(),
                        "primary-key record must not be null");
        for (RecordData.FieldGetter getter : primaryKeyGetters) {
            values.add(getter.getFieldOrNull(record));
        }
        return (Arrays.deepHashCode(values.toArray()) * 31) & 0x7FFFFFFF;
    }
}
