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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** SQL construction helpers that keep identifiers and values separate. */
@Internal
public final class DuckDbSqlUtils {

    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    public static String qualifiedName(String... identifiers) {
        return Arrays.stream(identifiers)
                .map(DuckDbSqlUtils::quoteIdentifier)
                .collect(Collectors.joining("."));
    }

    public static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    public static String primaryKeyPredicate(
            String tableAlias, String keyAlias, List<String> primaryKeys) {
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("Primary key columns must not be empty");
        }
        return primaryKeys.stream()
                .map(
                        column ->
                                tableAlias
                                        + "."
                                        + quoteIdentifier(column)
                                        + " IS NOT DISTINCT FROM "
                                        + keyAlias
                                        + "."
                                        + quoteIdentifier(column))
                .collect(Collectors.joining(" AND "));
    }

    private DuckDbSqlUtils() {}
}
