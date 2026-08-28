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

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/** Checkpoint commit payload containing a data file or schema change. */
@Internal
public final class DuckLakeCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sinkId;
    private final String operatorId;
    @Nullable private final DuckLakeWriteResult writeResult;
    @Nullable private final DuckLakeSchemaChangeResult schemaChangeResult;
    private final String payloadHash;

    public DuckLakeCommittable(
            String sinkId,
            String operatorId,
            DuckLakeWriteResult writeResult,
            String writeResultHash) {
        this.sinkId = Objects.requireNonNull(sinkId, "sinkId must not be null");
        this.operatorId = Objects.requireNonNull(operatorId, "operatorId must not be null");
        this.writeResult = Objects.requireNonNull(writeResult, "writeResult must not be null");
        this.schemaChangeResult = null;
        this.payloadHash = Objects.requireNonNull(writeResultHash, "payloadHash must not be null");
        validate();
    }

    public DuckLakeCommittable(
            String sinkId,
            String operatorId,
            DuckLakeSchemaChangeResult schemaChangeResult,
            String payloadHash) {
        this.sinkId = Objects.requireNonNull(sinkId, "sinkId must not be null");
        this.operatorId = Objects.requireNonNull(operatorId, "operatorId must not be null");
        this.writeResult = null;
        this.schemaChangeResult =
                Objects.requireNonNull(schemaChangeResult, "schemaChangeResult must not be null");
        this.payloadHash = Objects.requireNonNull(payloadHash, "payloadHash must not be null");
        validate();
    }

    private void validate() {
        if (sinkId.isEmpty() || operatorId.isEmpty() || payloadHash.isEmpty()) {
            throw new IllegalArgumentException(
                    "sinkId, operatorId, and payloadHash must not be empty");
        }
    }

    public String getSinkId() {
        return sinkId;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public DuckLakeWriteResult getWriteResult() {
        if (writeResult == null) {
            throw new IllegalStateException("Schema committable does not contain a write result");
        }
        return writeResult;
    }

    public DuckLakeSchemaChangeResult getSchemaChangeResult() {
        if (schemaChangeResult == null) {
            throw new IllegalStateException("Data committable does not contain a schema change");
        }
        return schemaChangeResult;
    }

    public boolean isSchemaChange() {
        return schemaChangeResult != null;
    }

    public String getPayloadHash() {
        return payloadHash;
    }

    public TableId getTableId() {
        return isSchemaChange()
                ? getSchemaChangeResult().getTableId()
                : getWriteResult().getTableId();
    }

    public long getCheckpointId() {
        return isSchemaChange()
                ? getSchemaChangeResult().getCheckpointId()
                : getWriteResult().getCheckpointId();
    }

    public int getSchemaBatchIndex() {
        return isSchemaChange()
                ? getSchemaChangeResult().getSchemaBatchIndex()
                : getWriteResult().getSchemaBatchIndex();
    }

    public int getSubtaskId() {
        return isSchemaChange()
                ? getSchemaChangeResult().getSubtaskId()
                : getWriteResult().getSubtaskId();
    }

    public int getAttemptNumber() {
        return isSchemaChange()
                ? getSchemaChangeResult().getAttemptNumber()
                : getWriteResult().getAttemptNumber();
    }

    public long getFileSequence() {
        return isSchemaChange() ? 0 : getWriteResult().getFileSequence();
    }

    DuckLakeCommittable withCheckpointId(long actualCheckpointId) throws IOException {
        if (getCheckpointId() == actualCheckpointId) {
            return this;
        }
        if (isSchemaChange()) {
            DuckLakeSchemaChangeResult normalized =
                    schemaChangeResult.withCheckpointId(actualCheckpointId);
            return new DuckLakeCommittable(
                    sinkId,
                    operatorId,
                    normalized,
                    DuckLakeCommittableSerializer.hashSchemaChangeResult(normalized));
        }
        DuckLakeWriteResult normalized = writeResult.withCheckpointId(actualCheckpointId);
        return new DuckLakeCommittable(
                sinkId,
                operatorId,
                normalized,
                DuckLakeCommittableSerializer.hashWriteResult(normalized));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DuckLakeCommittable)) {
            return false;
        }
        DuckLakeCommittable that = (DuckLakeCommittable) o;
        return sinkId.equals(that.sinkId)
                && operatorId.equals(that.operatorId)
                && Objects.equals(writeResult, that.writeResult)
                && Objects.equals(schemaChangeResult, that.schemaChangeResult)
                && payloadHash.equals(that.payloadHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sinkId, operatorId, writeResult, schemaChangeResult, payloadHash);
    }

    @Override
    public String toString() {
        return "DuckLakeCommittable{"
                + "sinkId='"
                + sinkId
                + '\''
                + ", operatorId='"
                + operatorId
                + '\''
                + ", writeResult="
                + writeResult
                + ", schemaChangeResult="
                + schemaChangeResult
                + ", payloadHash='"
                + payloadHash
                + '\''
                + '}';
    }
}
