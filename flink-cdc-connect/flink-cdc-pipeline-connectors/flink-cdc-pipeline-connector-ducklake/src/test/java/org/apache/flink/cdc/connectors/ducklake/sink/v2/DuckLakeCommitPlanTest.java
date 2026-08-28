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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.flink.cdc.connectors.ducklake.sink.v2.DuckLakeTestData.committable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeCommitPlanTest {

    @Test
    void sortsByTableBatchSequenceAndWriter() throws Exception {
        List<DuckLakeCommittable> input =
                Arrays.asList(
                        committable("z", 1, 1, 1, "z-1-1"),
                        committable("a", 2, 0, 0, "a-2-0"),
                        committable("a", 1, 1, 0, "a-1-1"),
                        committable("a", 1, 0, 1, "a-1-0-1"),
                        committable("a", 1, 0, 0, "a-1-0-0"));
        Collections.shuffle(input, new Random(17));

        DuckLakeCommitPlan plan = DuckLakeCommitPlan.from(input);

        assertThat(plan.getCommittables())
                .extracting(DuckLakeCommittable::getWriteResultHash)
                .containsExactly("a-1-0-0", "a-1-1", "a-1-0-1", "a-2-0", "z-1-1");
    }

    @Test
    void producesSameHashForEveryPermutation() throws Exception {
        List<DuckLakeCommittable> input =
                Arrays.asList(
                        committable("z", 1, 1, 1, "one"),
                        committable("a", 2, 0, 0, "two"),
                        committable("a", 1, 0, 0, "three"));
        String expected = DuckLakeCommitPlan.from(input).getPlanHash();

        for (int seed = 0; seed < 20; seed++) {
            List<DuckLakeCommittable> shuffled = new ArrayList<>(input);
            Collections.shuffle(shuffled, new Random(seed));
            assertThat(DuckLakeCommitPlan.from(shuffled).getPlanHash()).isEqualTo(expected);
        }
        assertThat(expected).hasSize(64);
    }

    @Test
    void deduplicatesSameOrderingKeyAndHash() throws Exception {
        DuckLakeCommittable committable = committable("a", 1, 0, 0, "same");

        assertThat(
                        DuckLakeCommitPlan.from(Arrays.asList(committable, committable))
                                .getCommittables())
                .containsExactly(committable);
    }

    @Test
    void rejectsConflictingDuplicateOrderingKey() {
        DuckLakeCommittable first = committable("a", 1, 0, 0, "first");
        DuckLakeCommittable conflict = committable("a", 1, 0, 0, "different");

        assertThatThrownBy(() -> DuckLakeCommitPlan.from(Arrays.asList(first, conflict)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate ordering key");
    }

    @Test
    void rejectsMixedSinkIdentity() {
        DuckLakeCommittable first = committable("a", 1, 0, 0, "first");

        assertThatThrownBy(
                        () ->
                                DuckLakeCommitPlan.from(
                                        Arrays.asList(
                                                first,
                                                new DuckLakeCommittable(
                                                        "other",
                                                        "operator-1",
                                                        first.getWriteResult(),
                                                        "first"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink instance");
    }
}
