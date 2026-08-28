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

import java.sql.SQLException;
import java.util.Locale;

/** Classifies catalog failures that are safe to retry from a fresh transaction. */
@Internal
public final class DuckLakeSqlExceptionUtils {

    public static boolean isRetryable(SQLException exception) {
        String state = exception.getSQLState();
        String message = String.valueOf(exception.getMessage()).toLowerCase(Locale.ROOT);
        return (state != null && state.startsWith("08"))
                || "40001".equals(state)
                || message.contains("serialization conflict")
                || message.contains("transaction conflict")
                || message.contains("snapshot conflict")
                || message.contains("catalog conflict");
    }

    public static boolean isConnectionFailure(SQLException exception) {
        String state = exception.getSQLState();
        return state != null && state.startsWith("08");
    }

    private DuckLakeSqlExceptionUtils() {}
}
