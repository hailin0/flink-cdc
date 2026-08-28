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
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeHashUtils;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Canonically ordered and hashed commit plan for one checkpoint. */
@Internal
public final class DuckLakeCommitPlan {

    private static final Comparator<TableId> TABLE_COMPARATOR =
            Comparator.comparing(
                            TableId::getNamespace, Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparing(
                            TableId::getSchemaName,
                            Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparing(TableId::getTableName);
    private static final Comparator<DuckLakeCommittable> ORDERING =
            Comparator.comparing(DuckLakeCommittable::getTableId, TABLE_COMPARATOR)
                    .thenComparingInt(DuckLakeCommittable::getSchemaBatchIndex)
                    .thenComparingLong(DuckLakeCommittable::getFileSequence)
                    .thenComparingInt(DuckLakeCommittable::getSubtaskId);

    private final String sinkId;
    private final String operatorId;
    private final long checkpointId;
    private final List<DuckLakeCommittable> committables;
    private final String planHash;

    private DuckLakeCommitPlan(
            String sinkId,
            String operatorId,
            long checkpointId,
            List<DuckLakeCommittable> committables,
            String planHash) {
        this.sinkId = sinkId;
        this.operatorId = operatorId;
        this.checkpointId = checkpointId;
        this.committables = Collections.unmodifiableList(committables);
        this.planHash = planHash;
    }

    public static DuckLakeCommitPlan from(Collection<DuckLakeCommittable> input)
            throws IOException {
        if (input.isEmpty()) {
            throw new IllegalArgumentException("DuckLake commit plan must not be empty");
        }
        List<DuckLakeCommittable> sorted = new ArrayList<>(input);
        DuckLakeCommittable first = sorted.get(0);
        for (DuckLakeCommittable committable : sorted) {
            if (!first.getSinkId().equals(committable.getSinkId())) {
                throw new IllegalArgumentException("Mixed sink instance IDs in commit plan");
            }
            if (!first.getOperatorId().equals(committable.getOperatorId())) {
                throw new IllegalArgumentException("Mixed operator IDs in commit plan");
            }
            if (first.getCheckpointId() != committable.getCheckpointId()) {
                throw new IllegalArgumentException("Mixed checkpoint IDs in commit plan");
            }
        }
        sorted.sort(ORDERING);
        List<DuckLakeCommittable> deduplicated = new ArrayList<>();
        for (DuckLakeCommittable current : sorted) {
            if (!deduplicated.isEmpty()) {
                DuckLakeCommittable previous = deduplicated.get(deduplicated.size() - 1);
                if (ORDERING.compare(previous, current) == 0) {
                    if (!previous.getWriteResultHash().equals(current.getWriteResultHash())) {
                        throw new IllegalArgumentException(
                                "Conflicting duplicate ordering key in DuckLake commit plan");
                    }
                    continue;
                }
            }
            deduplicated.add(current);
        }
        return new DuckLakeCommitPlan(
                first.getSinkId(),
                first.getOperatorId(),
                first.getCheckpointId(),
                deduplicated,
                hash(deduplicated));
    }

    public String getSinkId() {
        return sinkId;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public List<DuckLakeCommittable> getCommittables() {
        return committables;
    }

    public String getPlanHash() {
        return planHash;
    }

    private static String hash(List<DuckLakeCommittable> committables) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            DuckLakeCommittableSerializer serializer = new DuckLakeCommittableSerializer();
            updateInt(digest, serializer.getVersion());
            for (DuckLakeCommittable committable : committables) {
                byte[] bytes = serializer.serialize(committable);
                updateInt(digest, bytes.length);
                digest.update(bytes);
            }
            return DuckLakeHashUtils.toHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is not available", e);
        }
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }
}
