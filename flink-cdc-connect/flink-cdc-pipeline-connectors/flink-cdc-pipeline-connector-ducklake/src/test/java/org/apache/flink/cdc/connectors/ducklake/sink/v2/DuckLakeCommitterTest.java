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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeCommitterTest {

    @Test
    void skipsCheckpointWhenMatchingMarkerExists() throws Exception {
        DuckLakeCommitPlan plan = plan();
        TestingExecutor executor = new TestingExecutor();
        executor.existingHash = plan.getPlanHash();
        List<TestingCommitRequest> requests = requests(plan);

        new DuckLakeCommitter(executor, 2, Duration.ZERO).commit(new ArrayList<>(requests));

        assertThat(executor.transactionCalls).isEmpty();
        assertThat(requests).allSatisfy(request -> assertThat(request.committed).isTrue());
    }

    @Test
    void rejectsMarkerWithDifferentPlanHash() throws Exception {
        DuckLakeCommitPlan plan = plan();
        TestingExecutor executor = new TestingExecutor();
        executor.existingHash = "different";

        assertThatThrownBy(
                        () ->
                                new DuckLakeCommitter(executor, 2, Duration.ZERO)
                                        .commit(new ArrayList<>(requests(plan))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different commit plan hash");
    }

    @Test
    void rechecksMarkerAfterAcquiringCoordinationFence() throws Exception {
        DuckLakeCommitPlan plan = plan();
        TestingExecutor executor = new TestingExecutor();
        executor.hashVisibleAfterBegin = plan.getPlanHash();
        List<TestingCommitRequest> requests = requests(plan);

        new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests));

        assertThat(executor.findCalls).isEqualTo(2);
        assertThat(executor.transactionCalls).containsExactly("begin", "rollback");
        assertThat(requests).allSatisfy(request -> assertThat(request.committed).isTrue());
    }

    @Test
    void rollsBackTransactionAfterRuntimeFailure() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        executor.runtimeFailure = new IllegalArgumentException("invalid writeResult");

        assertThatThrownBy(
                        () ->
                                new DuckLakeCommitter(executor, 0, Duration.ZERO)
                                        .commit(new ArrayList<>(requests(plan()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid writeResult");
        assertThat(executor.transactionCalls).containsExactly("begin", "apply", "rollback");
    }

    @Test
    void retriesWholeTransactionAfterRetryableConflict() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        executor.failure = new SQLException("serialization conflict", "40001");
        executor.retryable = true;

        new DuckLakeCommitter(executor, 2, Duration.ZERO).commit(new ArrayList<>(requests(plan())));

        assertThat(executor.transactionCalls)
                .containsSubsequence(
                        "begin",
                        "apply",
                        "rollback",
                        "reset",
                        "begin",
                        "apply",
                        "marker",
                        "commit");
    }

    @Test
    void newCommitDoesNotSignalAlreadyCommitted() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        List<TestingCommitRequest> requests = requests(plan());

        new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests));

        assertThat(executor.transactionCalls).contains("commit");
        assertThat(requests).allSatisfy(request -> assertThat(request.committed).isFalse());
    }

    @Test
    void groupsSameTableSchemaBatchIntoOneTransactionApply() throws Exception {
        TestingExecutor executor = new TestingExecutor();

        new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests(plan())));

        assertThat(executor.transactionCalls).filteredOn("apply"::equals).hasSize(1);
    }

    @Test
    void appliesSuccessiveFileSegmentsInOrder() throws Exception {
        DuckLakeCommitPlan plan =
                DuckLakeCommitPlan.from(
                        Arrays.asList(
                                DuckLakeTestData.committable("a", 0, 0, 0, "before"),
                                DuckLakeTestData.committable("a", 0, 0, 1, "after")));
        TestingExecutor executor = new TestingExecutor();

        new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests(plan)));

        assertThat(executor.appliedFileSequences).containsExactly(0L, 1L);
    }

    @Test
    void appliesDataAndSchemaChangesInSchemaBatchOrder() throws Exception {
        DuckLakeCommitPlan plan =
                DuckLakeCommitPlan.from(
                        Arrays.asList(
                                DuckLakeTestData.committable("a", 0, 0, 0, "before"),
                                DuckLakeTestData.schemaChangeResult("a", 1, 0),
                                DuckLakeTestData.committable("a", 1, 0, 0, "after")));
        TestingExecutor executor = new TestingExecutor();

        new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests(plan)));

        assertThat(executor.transactionCalls)
                .containsSubsequence("begin", "apply", "schema", "apply", "marker", "commit");
    }

    @Test
    void groupsWritersByFileSegmentBeforeApplyingLaterSegments() throws Exception {
        DuckLakeCommitPlan plan =
                DuckLakeCommitPlan.from(
                        Arrays.asList(
                                DuckLakeTestData.committable("a", 0, 0, 0, "w0s0"),
                                DuckLakeTestData.committable("a", 0, 0, 1, "w0s1"),
                                DuckLakeTestData.committable("a", 0, 1, 0, "w1s0"),
                                DuckLakeTestData.committable("a", 0, 1, 1, "w1s1")));
        TestingExecutor executor = new TestingExecutor();

        new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests(plan)));

        assertThat(executor.appliedSegments)
                .containsExactly(Arrays.asList("0:0", "1:0"), Arrays.asList("0:1", "1:1"));
    }

    @Test
    void resolvesUnknownCommitResultAfterResettingConnection() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        executor.commitFailure = new SQLException("connection lost after commit", "08006");
        executor.retryable = true;
        List<TestingCommitRequest> requests = requests(plan());

        new DuckLakeCommitter(executor, 1, Duration.ZERO).commit(new ArrayList<>(requests));

        assertThat(executor.transactionCalls).containsSubsequence("commit", "reset");
        assertThat(requests).allSatisfy(request -> assertThat(request.committed).isTrue());
    }

    @Test
    void reconnectsAndRetriesWhenReadingMarkerFailsTransiently() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        executor.findFailure = new SQLException("catalog connection unavailable", "08001");
        executor.retryable = true;

        new DuckLakeCommitter(executor, 1, Duration.ZERO).commit(new ArrayList<>(requests(plan())));

        assertThat(executor.findCalls).isEqualTo(3);
        assertThat(executor.transactionCalls)
                .containsExactly("reset", "begin", "apply", "marker", "commit");
    }

    @Test
    void doesNotRetryLogicalConflict() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        executor.failure = new SQLException("logical conflict", "23000");

        assertThatThrownBy(
                        () ->
                                new DuckLakeCommitter(executor, 2, Duration.ZERO)
                                        .commit(new ArrayList<>(requests(plan()))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("logical conflict");
        assertThat(executor.transactionCalls).containsExactly("begin", "apply", "rollback");
    }

    @Test
    void reportsCommitterMetricsForRetryAndSuccess() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        executor.failure = new SQLException("serialization conflict", "40001");
        executor.retryable = true;
        TestingMetrics metrics = new TestingMetrics();

        new DuckLakeCommitter(executor, 1, Duration.ZERO, metrics.metricGroup())
                .commit(new ArrayList<>(requests(plan())));

        assertThat(metrics.count("getNumCommittablesTotalCounter")).isEqualTo(2);
        assertThat(metrics.count("getNumCommittablesRetryCounter")).isEqualTo(2);
        assertThat(metrics.count("getNumCommittablesSuccessCounter")).isEqualTo(2);
        assertThat(metrics.count("getNumCommittablesFailureCounter")).isZero();
    }

    @Test
    void cleansOldOrphanFilesOnlyAfterCheckpointCommit() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        AtomicLong clock = new AtomicLong();

        new DuckLakeCommitter(
                        executor,
                        0,
                        Duration.ZERO,
                        Duration.ofHours(1),
                        Duration.ofDays(7),
                        clock::get,
                        null)
                .commit(new ArrayList<>(requests(plan())));

        assertThat(executor.transactionCalls)
                .containsSubsequence("marker", "commit", "cleanup-orphans");
        assertThat(executor.orphanRetention).isEqualTo(Duration.ofDays(7));
    }

    @Test
    void orphanCleanupFailureDoesNotFailCommittedCheckpoint() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        executor.cleanupFailure = new SQLException("object store unavailable", "08001");

        new DuckLakeCommitter(
                        executor,
                        0,
                        Duration.ZERO,
                        Duration.ofHours(1),
                        Duration.ofDays(7),
                        () -> 0L,
                        null)
                .commit(new ArrayList<>(requests(plan())));

        assertThat(executor.transactionCalls)
                .containsSubsequence("marker", "commit", "cleanup-orphans");
    }

    @Test
    void throttlesOrphanCleanupAcrossCheckpoints() throws Exception {
        TestingExecutor executor = new TestingExecutor();
        AtomicLong clock = new AtomicLong();
        DuckLakeCommitter committer =
                new DuckLakeCommitter(
                        executor,
                        0,
                        Duration.ZERO,
                        Duration.ofHours(1),
                        Duration.ofDays(7),
                        clock::get,
                        null);

        committer.commit(new ArrayList<>(requests(planAt(8))));
        executor.existingHash = null;
        clock.set(Duration.ofMinutes(30).toMillis());
        committer.commit(new ArrayList<>(requests(planAt(9))));
        executor.existingHash = null;
        clock.set(Duration.ofHours(1).toMillis());
        committer.commit(new ArrayList<>(requests(planAt(10))));

        assertThat(executor.transactionCalls).filteredOn("cleanup-orphans"::equals).hasSize(2);
    }

    @Test
    void commitsAllWriterTablesBehindOneTransactionFence() throws Exception {
        DuckLakeCommitPlan plan =
                DuckLakeCommitPlan.from(
                        Arrays.asList(
                                DuckLakeTestData.committable("z", 0, 0, 0, "z"),
                                DuckLakeTestData.committable("a", 0, 1, 0, "a")));
        TestingExecutor executor = new TestingExecutor();

        new DuckLakeCommitter(executor, 0, Duration.ZERO).commit(new ArrayList<>(requests(plan)));

        assertThat(executor.transactionCalls)
                .filteredOn(call -> call.equals("begin") || call.equals("commit"))
                .containsExactly("begin", "commit");
        assertThat(executor.begunTables)
                .containsExactlyInAnyOrder(
                        TableId.tableId("schema", "a"), TableId.tableId("schema", "z"));
    }

    private static DuckLakeCommitPlan plan() throws Exception {
        return DuckLakeCommitPlan.from(
                Arrays.asList(
                        DuckLakeTestData.committable("a", 0, 0, 0, "one"),
                        DuckLakeTestData.committable("a", 0, 1, 0, "two")));
    }

    private static DuckLakeCommitPlan planAt(long checkpointId) throws Exception {
        List<DuckLakeCommittable> committables = new ArrayList<>();
        for (DuckLakeCommittable committable : plan().getCommittables()) {
            committables.add(committable.withCheckpointId(checkpointId));
        }
        return DuckLakeCommitPlan.from(committables);
    }

    private static List<TestingCommitRequest> requests(DuckLakeCommitPlan plan) {
        return plan.getCommittables().stream()
                .map(TestingCommitRequest::new)
                .collect(Collectors.toList());
    }

    private static final class TestingCommitRequest
            implements Committer.CommitRequest<DuckLakeCommittable> {
        private final DuckLakeCommittable committable;
        private boolean committed;

        private TestingCommitRequest(DuckLakeCommittable committable) {
            this.committable = committable;
        }

        @Override
        public DuckLakeCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable throwable) {}

        @Override
        public void signalFailedWithUnknownReason(Throwable throwable) {}

        @Override
        public void retryLater() {}

        @Override
        public void updateAndRetryLater(DuckLakeCommittable committable) {}

        @Override
        public void signalAlreadyCommitted() {
            committed = true;
        }
    }

    private static final class TestingMetrics {
        private final Map<String, Counter> counters = new HashMap<>();

        private SinkCommitterMetricGroup metricGroup() {
            return (SinkCommitterMetricGroup)
                    Proxy.newProxyInstance(
                            DuckLakeCommitterTest.class.getClassLoader(),
                            new Class<?>[] {SinkCommitterMetricGroup.class},
                            (proxy, method, args) -> {
                                if (method.getName().startsWith("getNumCommittables")) {
                                    return counters.computeIfAbsent(
                                            method.getName(), ignored -> new SimpleCounter());
                                }
                                return null;
                            });
        }

        private long count(String name) {
            return counters.getOrDefault(name, new SimpleCounter()).getCount();
        }
    }

    private static final class TestingExecutor implements DuckLakeCommitExecutor {
        private final List<String> transactionCalls = new ArrayList<>();
        private final List<Long> appliedFileSequences = new ArrayList<>();
        private final List<List<String>> appliedSegments = new ArrayList<>();
        private String existingHash;
        private String pendingHash;
        private SQLException failure;
        private SQLException findFailure;
        private SQLException commitFailure;
        private SQLException cleanupFailure;
        private RuntimeException runtimeFailure;
        private String hashVisibleAfterBegin;
        private boolean retryable;
        private boolean poisoned;
        private int findCalls;
        private Duration orphanRetention;
        private List<TableId> begunTables;

        @Override
        public Optional<String> findCommittedPlanHash(
                String sinkId, String operatorId, long checkpointId) throws SQLException {
            findCalls++;
            if (findFailure != null) {
                SQLException current = findFailure;
                findFailure = null;
                throw current;
            }
            if (poisoned) {
                throw new SQLException("connection is closed", "08003");
            }
            return Optional.ofNullable(existingHash);
        }

        @Override
        public void beginCheckpoint(String sinkId, String operatorId, List<TableId> tables) {
            transactionCalls.add("begin");
            begunTables = new ArrayList<>(tables);
            if (hashVisibleAfterBegin != null) {
                existingHash = hashVisibleAfterBegin;
                hashVisibleAfterBegin = null;
            }
        }

        @Override
        public void applyTableChanges(List<DuckLakeCommittable> committables) throws SQLException {
            transactionCalls.add("apply");
            appliedFileSequences.add(committables.get(0).getFileSequence());
            appliedSegments.add(
                    committables.stream()
                            .map(
                                    committable ->
                                            committable.getSubtaskId()
                                                    + ":"
                                                    + committable.getFileSequence())
                            .collect(Collectors.toList()));
            if (runtimeFailure != null) {
                RuntimeException current = runtimeFailure;
                runtimeFailure = null;
                throw current;
            }
            if (failure != null) {
                SQLException current = failure;
                failure = null;
                throw current;
            }
        }

        @Override
        public void applySchemaChange(DuckLakeSchemaChangeResult schemaChangeResult) {
            transactionCalls.add("schema");
        }

        @Override
        public void recordCheckpoint(
                String sinkId, String operatorId, long checkpointId, String planHash) {
            transactionCalls.add("marker");
            pendingHash = planHash;
        }

        @Override
        public void commit() throws SQLException {
            transactionCalls.add("commit");
            existingHash = pendingHash;
            pendingHash = null;
            if (commitFailure != null) {
                SQLException current = commitFailure;
                commitFailure = null;
                poisoned = true;
                throw current;
            }
        }

        @Override
        public void rollback() {
            transactionCalls.add("rollback");
            pendingHash = null;
        }

        @Override
        public void resetConnection() {
            transactionCalls.add("reset");
            poisoned = false;
        }

        @Override
        public void cleanupOrphanFiles(Duration retention) throws SQLException {
            transactionCalls.add("cleanup-orphans");
            orphanRetention = retention;
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
        }

        @Override
        public boolean isRetryable(SQLException exception) {
            return retryable;
        }
    }
}
