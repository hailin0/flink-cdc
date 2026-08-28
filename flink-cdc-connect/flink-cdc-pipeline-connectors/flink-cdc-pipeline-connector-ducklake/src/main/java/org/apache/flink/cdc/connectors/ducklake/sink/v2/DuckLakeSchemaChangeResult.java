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
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.event.SchemaChangeEventSerializer;

import java.io.Serializable;
import java.util.Objects;

/** A schema change that enters a table schema batch at a checkpoint boundary. */
@Internal
public final class DuckLakeSchemaChangeResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;
    private final int schemaBatchIndex;
    private final int subtaskId;
    private final int attemptNumber;
    private final SchemaChangeEvent event;

    public DuckLakeSchemaChangeResult(
            long checkpointId,
            int schemaBatchIndex,
            int subtaskId,
            int attemptNumber,
            SchemaChangeEvent event) {
        if (checkpointId < 0 || schemaBatchIndex < 0 || subtaskId < 0 || attemptNumber < 0) {
            throw new IllegalArgumentException(
                    "Checkpoint, schema batch, subtask, and attempt must be non-negative");
        }
        this.checkpointId = checkpointId;
        this.schemaBatchIndex = schemaBatchIndex;
        this.subtaskId = subtaskId;
        this.attemptNumber = attemptNumber;
        this.event =
                SchemaChangeEventSerializer.INSTANCE.copy(
                        Objects.requireNonNull(event, "event must not be null"));
    }

    public TableId getTableId() {
        return event.tableId();
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

    public SchemaChangeEvent getEvent() {
        return SchemaChangeEventSerializer.INSTANCE.copy(event);
    }

    DuckLakeSchemaChangeResult withCheckpointId(long actualCheckpointId) {
        if (checkpointId == actualCheckpointId) {
            return this;
        }
        return new DuckLakeSchemaChangeResult(
                actualCheckpointId, schemaBatchIndex, subtaskId, attemptNumber, event);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DuckLakeSchemaChangeResult)) {
            return false;
        }
        DuckLakeSchemaChangeResult that = (DuckLakeSchemaChangeResult) o;
        return checkpointId == that.checkpointId
                && schemaBatchIndex == that.schemaBatchIndex
                && subtaskId == that.subtaskId
                && attemptNumber == that.attemptNumber
                && event.equals(that.event);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointId, schemaBatchIndex, subtaskId, attemptNumber, event);
    }

    @Override
    public String toString() {
        return "DuckLakeSchemaChangeResult{"
                + "checkpointId="
                + checkpointId
                + ", schemaBatchIndex="
                + schemaBatchIndex
                + ", subtaskId="
                + subtaskId
                + ", attemptNumber="
                + attemptNumber
                + ", event="
                + event
                + '}';
    }
}
