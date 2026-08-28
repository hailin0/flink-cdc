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
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeRetryBackoff;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/** Globally coordinated, retryable, and idempotent DuckLake checkpoint committer. */
@Internal
public final class DuckLakeCommitter implements Committer<DuckLakeCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DuckLakeCommitter.class);

    private final DuckLakeCommitExecutor executor;
    private final int maxRetries;
    private final long initialBackoffMillis;
    private final long orphanCleanupIntervalMillis;
    private final Duration orphanRetention;
    private final LongSupplier clock;
    @Nullable private final SinkCommitterMetricGroup metricGroup;
    private long nextOrphanCleanupMillis;

    public DuckLakeCommitter(
            DuckLakeCommitExecutor executor, int maxRetries, Duration initialBackoff) {
        this(
                executor,
                maxRetries,
                initialBackoff,
                Duration.ZERO,
                Duration.ofDays(7),
                System::currentTimeMillis,
                null);
    }

    public DuckLakeCommitter(
            DuckLakeCommitExecutor executor,
            int maxRetries,
            Duration initialBackoff,
            @Nullable SinkCommitterMetricGroup metricGroup) {
        this(
                executor,
                maxRetries,
                initialBackoff,
                Duration.ZERO,
                Duration.ofDays(7),
                System::currentTimeMillis,
                metricGroup);
    }

    DuckLakeCommitter(
            DuckLakeCommitExecutor executor,
            int maxRetries,
            Duration initialBackoff,
            Duration orphanCleanupInterval,
            Duration orphanRetention,
            LongSupplier clock,
            @Nullable SinkCommitterMetricGroup metricGroup) {
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        Objects.requireNonNull(initialBackoff, "initialBackoff must not be null");
        Objects.requireNonNull(orphanCleanupInterval, "orphanCleanupInterval must not be null");
        this.orphanRetention =
                Objects.requireNonNull(orphanRetention, "orphanRetention must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        if (maxRetries < 0
                || initialBackoff.isNegative()
                || orphanCleanupInterval.isNegative()
                || orphanRetention.compareTo(Duration.ofSeconds(1)) < 0) {
            throw new IllegalArgumentException(
                    "Retry/backoff/cleanup interval must not be negative and orphan retention "
                            + "must be at least one second");
        }
        this.maxRetries = maxRetries;
        this.initialBackoffMillis = initialBackoff.toMillis();
        this.orphanCleanupIntervalMillis = orphanCleanupInterval.toMillis();
        this.nextOrphanCleanupMillis = Long.MIN_VALUE;
        this.metricGroup = metricGroup;
    }

    @Override
    public void commit(Collection<CommitRequest<DuckLakeCommittable>> requests) throws IOException {
        if (metricGroup != null) {
            metricGroup.getNumCommittablesTotalCounter().inc(requests.size());
        }
        Map<Long, List<CommitRequest<DuckLakeCommittable>>> byCheckpoint = new TreeMap<>();
        for (CommitRequest<DuckLakeCommittable> request : requests) {
            byCheckpoint
                    .computeIfAbsent(
                            request.getCommittable().getCheckpointId(),
                            ignored -> new ArrayList<>())
                    .add(request);
        }
        for (List<CommitRequest<DuckLakeCommittable>> checkpointRequests : byCheckpoint.values()) {
            try {
                List<DuckLakeCommittable> committables = new ArrayList<>();
                for (CommitRequest<DuckLakeCommittable> request : checkpointRequests) {
                    committables.add(request.getCommittable());
                }
                DuckLakeCommitPlan plan = DuckLakeCommitPlan.from(committables);
                if (commitPlan(plan)) {
                    checkpointRequests.forEach(CommitRequest::signalAlreadyCommitted);
                    incrementAlreadyCommitted(checkpointRequests.size());
                } else {
                    incrementSuccess(checkpointRequests.size());
                }
                cleanupOrphansIfDue();
            } catch (IOException | RuntimeException failure) {
                incrementFailure(checkpointRequests.size());
                throw failure;
            }
        }
    }

    private void cleanupOrphansIfDue() {
        if (orphanCleanupIntervalMillis == 0) {
            return;
        }
        long now = clock.getAsLong();
        if (now < nextOrphanCleanupMillis) {
            return;
        }
        nextOrphanCleanupMillis = saturatedAdd(now, orphanCleanupIntervalMillis);
        try {
            executor.cleanupOrphanFiles(orphanRetention);
        } catch (SQLException | RuntimeException failure) {
            LOG.warn(
                    "Failed to clean DuckLake orphan files older than {}. "
                            + "The checkpoint is already committed and will not be retried.",
                    orphanRetention,
                    failure);
        }
    }

    private static long saturatedAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private boolean commitPlan(DuckLakeCommitPlan plan) throws IOException {
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            boolean begun = false;
            try {
                Optional<String> existing =
                        executor.findCommittedPlanHash(
                                plan.getSinkId(), plan.getOperatorId(), plan.getCheckpointId());
                if (existing.isPresent()) {
                    validateExistingPlan(plan, existing.get());
                    return true;
                }
                List<TableId> tables =
                        plan.getCommittables().stream()
                                .map(DuckLakeCommittable::getTableId)
                                .distinct()
                                .collect(Collectors.toList());
                executor.beginCheckpoint(plan.getSinkId(), plan.getOperatorId(), tables);
                begun = true;
                existing =
                        executor.findCommittedPlanHash(
                                plan.getSinkId(), plan.getOperatorId(), plan.getCheckpointId());
                if (existing.isPresent()) {
                    validateExistingPlan(plan, existing.get());
                    executor.rollback();
                    begun = false;
                    return true;
                }
                applyChanges(plan.getCommittables());
                executor.recordCheckpoint(
                        plan.getSinkId(),
                        plan.getOperatorId(),
                        plan.getCheckpointId(),
                        plan.getPlanHash());
                executor.commit();
                begun = false;
                return false;
            } catch (SQLException failure) {
                if (begun) {
                    try {
                        executor.rollback();
                    } catch (SQLException rollbackFailure) {
                        failure.addSuppressed(rollbackFailure);
                    }
                }
                if (!executor.isRetryable(failure) || attempt == maxRetries) {
                    throw checkpointFailure(plan, failure);
                }
                if (metricGroup != null) {
                    metricGroup.getNumCommittablesRetryCounter().inc(plan.getCommittables().size());
                }
                LOG.warn(
                        "Retrying DuckLake checkpoint {} after failed attempt {} of {}",
                        plan.getCheckpointId(),
                        attempt + 1,
                        maxRetries + 1,
                        failure);
                try {
                    executor.resetConnection();
                } catch (SQLException resetFailure) {
                    failure.addSuppressed(resetFailure);
                    throw new IOException(
                            "Failed to reset DuckLake commit executor after checkpoint "
                                    + plan.getCheckpointId(),
                            failure);
                }
                sleepBeforeRetry(attempt);
            } catch (RuntimeException failure) {
                if (begun) {
                    try {
                        executor.rollback();
                    } catch (SQLException rollbackFailure) {
                        failure.addSuppressed(rollbackFailure);
                    }
                }
                throw failure;
            }
        }
        throw new IllegalStateException("Unreachable DuckLake commit retry state");
    }

    private static void validateExistingPlan(DuckLakeCommitPlan plan, String existingHash) {
        if (!existingHash.equals(plan.getPlanHash())) {
            throw new IllegalStateException(
                    "Checkpoint has a different commit plan hash: " + plan.getCheckpointId());
        }
    }

    private void incrementSuccess(int count) {
        if (metricGroup != null) {
            metricGroup.getNumCommittablesSuccessCounter().inc(count);
        }
    }

    private void incrementAlreadyCommitted(int count) {
        if (metricGroup != null) {
            metricGroup.getNumCommittablesAlreadyCommittedCounter().inc(count);
        }
    }

    private void incrementFailure(int count) {
        if (metricGroup != null) {
            metricGroup.getNumCommittablesFailureCounter().inc(count);
        }
    }

    private static IOException checkpointFailure(DuckLakeCommitPlan plan, SQLException failure) {
        return new IOException(
                "Failed to commit DuckLake checkpoint "
                        + plan.getCheckpointId()
                        + ": "
                        + failure.getMessage(),
                failure);
    }

    private void applyChanges(List<DuckLakeCommittable> committables) throws SQLException {
        int index = 0;
        while (index < committables.size()) {
            DuckLakeCommittable committable = committables.get(index);
            if (committable.isSchemaChange()) {
                executor.applySchemaChange(committable.getSchemaChangeResult());
                index++;
                continue;
            }

            List<DuckLakeCommittable> batch = new ArrayList<>();
            batch.add(committable);
            index++;
            while (index < committables.size()
                    && sameDataBatch(committable, committables.get(index))) {
                batch.add(committables.get(index));
                index++;
            }
            executor.applyTableChanges(batch);
        }
    }

    private static boolean sameDataBatch(DuckLakeCommittable left, DuckLakeCommittable right) {
        return !right.isSchemaChange()
                && left.getTableId().equals(right.getTableId())
                && left.getSchemaBatchIndex() == right.getSchemaBatchIndex()
                && left.getFileSequence() == right.getFileSequence();
    }

    private void sleepBeforeRetry(int attempt) throws IOException {
        try {
            DuckLakeRetryBackoff.sleep(initialBackoffMillis, attempt, Thread::sleep);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while retrying DuckLake commit", e);
        }
    }

    @Override
    public void close() throws Exception {
        executor.close();
    }
}
