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

package org.apache.flink.cdc.connectors.ducklake.sink.provider;

import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** Creates temporary DuckDB secrets for one embedded connection. */
final class DuckDbSecretUtils {

    private DuckDbSecretUtils() {}

    static void createOrReplace(Connection connection, String name, List<String> parameters)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE SECRET "
                            + DuckDbSqlUtils.quoteIdentifier(name)
                            + " ("
                            + String.join(", ", parameters)
                            + ")");
        }
    }
}
