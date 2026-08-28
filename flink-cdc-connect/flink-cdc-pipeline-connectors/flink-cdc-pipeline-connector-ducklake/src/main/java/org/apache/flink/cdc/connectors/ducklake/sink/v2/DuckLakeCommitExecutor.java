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

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Transaction boundary used by the idempotent DuckLake committer. */
@Internal
public interface DuckLakeCommitExecutor extends AutoCloseable {

    Optional<String> findCommittedPlanHash(String sinkId, String operatorId, long checkpointId)
            throws SQLException;

    void beginCheckpoint(String sinkId, String operatorId, List<TableId> tables)
            throws SQLException;

    void applyTableChanges(List<DuckLakeCommittable> committables) throws SQLException;

    void recordCheckpoint(String sinkId, String operatorId, long checkpointId, String planHash)
            throws SQLException;

    void commit() throws SQLException;

    void rollback() throws SQLException;

    boolean isRetryable(SQLException exception);

    void resetConnection() throws SQLException;

    void cleanupOrphanFiles(Duration retention) throws SQLException;

    @Override
    default void close() throws Exception {}
}
