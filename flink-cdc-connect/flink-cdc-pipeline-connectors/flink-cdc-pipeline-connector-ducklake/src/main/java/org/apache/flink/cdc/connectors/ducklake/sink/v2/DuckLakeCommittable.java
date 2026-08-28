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

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/** Checkpoint commit payload for one completed DuckLake write result. */
@Internal
public final class DuckLakeCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sinkId;
    private final String operatorId;
    private final DuckLakeWriteResult writeResult;
    private final String writeResultHash;

    public DuckLakeCommittable(
            String sinkId,
            String operatorId,
            DuckLakeWriteResult writeResult,
            String writeResultHash) {
        this.sinkId = Objects.requireNonNull(sinkId, "sinkId must not be null");
        this.operatorId = Objects.requireNonNull(operatorId, "operatorId must not be null");
        this.writeResult = Objects.requireNonNull(writeResult, "writeResult must not be null");
        this.writeResultHash =
                Objects.requireNonNull(writeResultHash, "writeResultHash must not be null");
        if (sinkId.isEmpty() || operatorId.isEmpty() || writeResultHash.isEmpty()) {
            throw new IllegalArgumentException(
                    "sinkId, operatorId, and writeResultHash must not be empty");
        }
    }

    public String getSinkId() {
        return sinkId;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public DuckLakeWriteResult getWriteResult() {
        return writeResult;
    }

    public String getWriteResultHash() {
        return writeResultHash;
    }

    public TableId getTableId() {
        return writeResult.getTableId();
    }

    public long getCheckpointId() {
        return writeResult.getCheckpointId();
    }

    public int getSchemaBatchIndex() {
        return writeResult.getSchemaBatchIndex();
    }

    public int getSubtaskId() {
        return writeResult.getSubtaskId();
    }

    public int getAttemptNumber() {
        return writeResult.getAttemptNumber();
    }

    public long getFileSequence() {
        return writeResult.getFileSequence();
    }

    DuckLakeCommittable withCheckpointId(long actualCheckpointId) throws IOException {
        if (getCheckpointId() == actualCheckpointId) {
            return this;
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
                && writeResult.equals(that.writeResult)
                && writeResultHash.equals(that.writeResultHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sinkId, operatorId, writeResult, writeResultHash);
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
                + ", writeResultHash='"
                + writeResultHash
                + '\''
                + '}';
    }
}
