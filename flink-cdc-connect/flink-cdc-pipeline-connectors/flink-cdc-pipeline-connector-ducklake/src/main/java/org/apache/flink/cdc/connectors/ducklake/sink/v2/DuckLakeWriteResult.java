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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable description of one pair of writer-created Parquet files. */
@Internal
public final class DuckLakeWriteResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final long checkpointId;
    private final int schemaBatchIndex;
    private final int subtaskId;
    private final int attemptNumber;
    private final long fileSequence;
    private final List<DuckLakeColumnMetadata> columns;
    private final List<String> primaryKeys;
    @Nullable private final String dataFilePath;
    private final long dataRowCount;
    private final String keyFilePath;
    private final long keyRowCount;

    public DuckLakeWriteResult(
            TableId tableId,
            long checkpointId,
            int schemaBatchIndex,
            int subtaskId,
            int attemptNumber,
            long fileSequence,
            List<DuckLakeColumnMetadata> columns,
            List<String> primaryKeys,
            @Nullable String dataFilePath,
            long dataRowCount,
            String keyFilePath,
            long keyRowCount) {
        this.tableId = Objects.requireNonNull(tableId, "tableId must not be null");
        this.checkpointId = requireNonNegative(checkpointId, "checkpointId");
        this.schemaBatchIndex = requireNonNegative(schemaBatchIndex, "schemaBatchIndex");
        this.subtaskId = requireNonNegative(subtaskId, "subtaskId");
        this.attemptNumber = requireNonNegative(attemptNumber, "attemptNumber");
        this.fileSequence = requireNonNegative(fileSequence, "fileSequence");
        this.columns = immutableNonEmptyCopy(columns, "columns");
        this.primaryKeys = immutableNonEmptyCopy(primaryKeys, "primaryKeys");
        this.dataFilePath = dataFilePath;
        this.dataRowCount = requireNonNegative(dataRowCount, "dataRowCount");
        this.keyFilePath = Objects.requireNonNull(keyFilePath, "keyFilePath must not be null");
        this.keyRowCount = requireNonNegative(keyRowCount, "keyRowCount");
        if (dataFilePath == null && dataRowCount != 0) {
            throw new IllegalArgumentException(
                    "dataRowCount must be zero when dataFilePath is null");
        }
    }

    public TableId getTableId() {
        return tableId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public int getSchemaBatchIndex() {
        return schemaBatchIndex;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public long getFileSequence() {
        return fileSequence;
    }

    public List<DuckLakeColumnMetadata> getColumns() {
        return columns;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @Nullable
    public String getDataFilePath() {
        return dataFilePath;
    }

    public long getDataRowCount() {
        return dataRowCount;
    }

    public String getKeyFilePath() {
        return keyFilePath;
    }

    public long getKeyRowCount() {
        return keyRowCount;
    }

    DuckLakeWriteResult withCheckpointId(long actualCheckpointId) {
        if (checkpointId == actualCheckpointId) {
            return this;
        }
        return new DuckLakeWriteResult(
                tableId,
                actualCheckpointId,
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DuckLakeWriteResult)) {
            return false;
        }
        DuckLakeWriteResult that = (DuckLakeWriteResult) o;
        return checkpointId == that.checkpointId
                && schemaBatchIndex == that.schemaBatchIndex
                && subtaskId == that.subtaskId
                && attemptNumber == that.attemptNumber
                && fileSequence == that.fileSequence
                && dataRowCount == that.dataRowCount
                && keyRowCount == that.keyRowCount
                && tableId.equals(that.tableId)
                && columns.equals(that.columns)
                && primaryKeys.equals(that.primaryKeys)
                && Objects.equals(dataFilePath, that.dataFilePath)
                && keyFilePath.equals(that.keyFilePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
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

    @Override
    public String toString() {
        return "DuckLakeWriteResult{"
                + "tableId="
                + tableId
                + ", checkpointId="
                + checkpointId
                + ", schemaBatchIndex="
                + schemaBatchIndex
                + ", subtaskId="
                + subtaskId
                + ", attemptNumber="
                + attemptNumber
                + ", fileSequence="
                + fileSequence
                + ", columns="
                + columns
                + ", primaryKeys="
                + primaryKeys
                + ", dataFilePath='"
                + dataFilePath
                + '\''
                + ", dataRowCount="
                + dataRowCount
                + ", keyFilePath='"
                + keyFilePath
                + '\''
                + ", keyRowCount="
                + keyRowCount
                + '}';
    }

    private static int requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static long requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static <T> List<T> immutableNonEmptyCopy(List<T> values, String name) {
        Objects.requireNonNull(values, name + " must not be null");
        if (values.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        List<T> copy = new ArrayList<>(values.size());
        for (T value : values) {
            copy.add(Objects.requireNonNull(value, name + " must not contain null"));
        }
        return Collections.unmodifiableList(copy);
    }
}
