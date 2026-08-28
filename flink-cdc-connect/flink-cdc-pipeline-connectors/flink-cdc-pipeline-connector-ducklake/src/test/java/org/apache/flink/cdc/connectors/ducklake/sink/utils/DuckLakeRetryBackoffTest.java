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

package org.apache.flink.cdc.connectors.ducklake.sink.utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DuckLakeRetryBackoff}. */
class DuckLakeRetryBackoffTest {

    @Test
    void computesSaturatedExponentialDelays() {
        assertThat(DuckLakeRetryBackoff.delayMillis(100, 0)).isEqualTo(100);
        assertThat(DuckLakeRetryBackoff.delayMillis(100, 3)).isEqualTo(800);
        assertThat(DuckLakeRetryBackoff.delayMillis(Long.MAX_VALUE / 2 + 1, 1))
                .isEqualTo(Long.MAX_VALUE);
        assertThat(DuckLakeRetryBackoff.delayMillis(1, Integer.MAX_VALUE))
                .isEqualTo(Long.MAX_VALUE);
        assertThat(DuckLakeRetryBackoff.delayMillis(0, Integer.MAX_VALUE)).isZero();
    }

    @Test
    void skipsZeroDelayAndDelegatesPositiveDelay() throws Exception {
        List<Long> delays = new ArrayList<>();

        DuckLakeRetryBackoff.sleep(0, 5, delays::add);
        DuckLakeRetryBackoff.sleep(25, 2, delays::add);

        assertThat(delays).containsExactly(100L);
    }
}
