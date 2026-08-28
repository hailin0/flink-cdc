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

import org.apache.flink.cdc.common.annotation.Internal;

/** Computes and applies the exponential backoff shared by DuckLake retry loops. */
@Internal
public final class DuckLakeRetryBackoff {

    private DuckLakeRetryBackoff() {}

    public static long delayMillis(long initialDelayMillis, int attempt) {
        if (initialDelayMillis < 0 || attempt < 0) {
            throw new IllegalArgumentException("Retry delay and attempt must not be negative");
        }
        if (initialDelayMillis == 0) {
            return 0;
        }
        long delay = initialDelayMillis;
        for (int i = 0; i < attempt; i++) {
            if (delay > Long.MAX_VALUE / 2) {
                return Long.MAX_VALUE;
            }
            delay *= 2;
        }
        return delay;
    }

    public static void sleep(long initialDelayMillis, int attempt, Sleeper sleeper)
            throws InterruptedException {
        long delay = delayMillis(initialDelayMillis, attempt);
        if (delay > 0) {
            sleeper.sleep(delay);
        }
    }

    /** Performs the actual wait, allowing retry behavior to be tested without blocking. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }
}
