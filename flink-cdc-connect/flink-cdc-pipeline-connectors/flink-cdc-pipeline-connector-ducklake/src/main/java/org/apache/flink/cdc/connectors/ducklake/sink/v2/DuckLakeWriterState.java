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
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Recoverable state owned by one DuckLake sink writer.
 *
 * <p>The writer epoch only makes staging paths unique. The pre-commit topology replaces it with
 * Flink's actual checkpoint lineage before commit.
 */
@Internal
public final class DuckLakeWriterState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sinkId;
    private final String operatorId;
    private final long writerEpoch;
    private final Map<TableId, Schema> currentSchemas;
    private final Map<TableId, Integer> schemaBatchIndexes;
    private final Map<TableId, Long> fileSequences;
    private final List<DuckLakeWriteResult> pendingWriteResults;

    public DuckLakeWriterState(
            String sinkId,
            String operatorId,
            long writerEpoch,
            Map<TableId, Schema> currentSchemas,
            Map<TableId, Integer> schemaBatchIndexes,
            Map<TableId, Long> fileSequences,
            List<DuckLakeWriteResult> pendingWriteResults) {
        this.sinkId = Objects.requireNonNull(sinkId, "sinkId must not be null");
        this.operatorId = Objects.requireNonNull(operatorId, "operatorId must not be null");
        if (sinkId.isEmpty() || operatorId.isEmpty()) {
            throw new IllegalArgumentException("sinkId and operatorId must not be empty");
        }
        if (writerEpoch < 0) {
            throw new IllegalArgumentException("writerEpoch must be non-negative");
        }
        this.writerEpoch = writerEpoch;
        this.currentSchemas = immutableSchemaCopy(currentSchemas);
        this.schemaBatchIndexes = immutableMapCopy(schemaBatchIndexes, "schemaBatchIndexes");
        this.fileSequences = immutableMapCopy(fileSequences, "fileSequences");
        this.pendingWriteResults = immutableListCopy(pendingWriteResults, "pendingWriteResults");
    }

    public String getSinkId() {
        return sinkId;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public long getWriterEpoch() {
        return writerEpoch;
    }

    public Map<TableId, Schema> getCurrentSchemas() {
        return currentSchemas;
    }

    public Map<TableId, Integer> getSchemaBatchIndexes() {
        return schemaBatchIndexes;
    }

    public Map<TableId, Long> getFileSequences() {
        return fileSequences;
    }

    public List<DuckLakeWriteResult> getPendingWriteResults() {
        return pendingWriteResults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DuckLakeWriterState)) {
            return false;
        }
        DuckLakeWriterState that = (DuckLakeWriterState) o;
        return writerEpoch == that.writerEpoch
                && sinkId.equals(that.sinkId)
                && operatorId.equals(that.operatorId)
                && currentSchemas.equals(that.currentSchemas)
                && schemaBatchIndexes.equals(that.schemaBatchIndexes)
                && fileSequences.equals(that.fileSequences)
                && pendingWriteResults.equals(that.pendingWriteResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sinkId,
                operatorId,
                writerEpoch,
                currentSchemas,
                schemaBatchIndexes,
                fileSequences,
                pendingWriteResults);
    }

    @Override
    public String toString() {
        return "DuckLakeWriterState{"
                + "sinkId='"
                + sinkId
                + '\''
                + ", operatorId='"
                + operatorId
                + '\''
                + ", writerEpoch="
                + writerEpoch
                + ", currentSchemas="
                + currentSchemas.keySet()
                + ", schemaBatchIndexes="
                + schemaBatchIndexes
                + ", fileSequences="
                + fileSequences
                + ", pendingWriteResults="
                + pendingWriteResults
                + '}';
    }

    private static Map<TableId, Schema> immutableSchemaCopy(Map<TableId, Schema> values) {
        Objects.requireNonNull(values, "currentSchemas must not be null");
        Map<TableId, Schema> copy = new LinkedHashMap<>();
        for (Map.Entry<TableId, Schema> entry : values.entrySet()) {
            TableId key =
                    Objects.requireNonNull(entry.getKey(), "currentSchemas key must not be null");
            Schema value =
                    Objects.requireNonNull(
                            entry.getValue(), "currentSchemas value must not be null");
            copy.put(key, SchemaSerializer.INSTANCE.copy(value));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static <T> Map<TableId, T> immutableMapCopy(Map<TableId, T> values, String name) {
        Objects.requireNonNull(values, name + " must not be null");
        Map<TableId, T> copy = new LinkedHashMap<>();
        for (Map.Entry<TableId, T> entry : values.entrySet()) {
            copy.put(
                    Objects.requireNonNull(entry.getKey(), name + " key must not be null"),
                    Objects.requireNonNull(entry.getValue(), name + " value must not be null"));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static <T> List<T> immutableListCopy(List<T> values, String name) {
        Objects.requireNonNull(values, name + " must not be null");
        List<T> copy = new ArrayList<>(values.size());
        for (T value : values) {
            copy.add(Objects.requireNonNull(value, name + " must not contain null"));
        }
        return Collections.unmodifiableList(copy);
    }
}
