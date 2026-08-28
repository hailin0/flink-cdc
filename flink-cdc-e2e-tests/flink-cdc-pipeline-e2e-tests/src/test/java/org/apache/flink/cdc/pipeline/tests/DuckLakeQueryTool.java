/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.cdc.pipeline.tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Runs DuckLake verification queries inside a Flink test container. */
class DuckLakeQueryTool {

    static final String OUTPUT_PREFIX = "DUCKLAKE_RESULT=";

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String table = qualifiedTable(args[10], args[11]);

        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            execute(connection, "SET extension_directory = '/opt/flink/.duckdb/extensions'");
            installAndLoad(connection, "httpfs");
            installAndLoad(connection, "postgres");
            installAndLoad(connection, "ducklake");
            try (Statement statement = connection.createStatement()) {
                statement.execute(s3Secret(args[6], args[7], args[8], args[9]));
                statement.execute(attachStatement(args));

                if ("rows".equals(mode)) {
                    printRows(statement, table);
                } else if ("rows-at-snapshot".equals(mode)) {
                    printRows(
                            statement, table + " AT (VERSION => " + Long.parseLong(args[12]) + ")");
                } else if ("column-type".equals(mode)) {
                    printColumnType(statement, table, args[12]);
                } else if ("flink-cdc-snapshots".equals(mode)) {
                    printFlinkCdcSnapshots(statement);
                } else {
                    throw new IllegalArgumentException("Unknown query mode: " + mode);
                }
            }
        }
    }

    private static void installAndLoad(Connection connection, String extension)
            throws SQLException {
        try {
            execute(connection, "LOAD " + extension);
        } catch (SQLException loadFailure) {
            try {
                execute(connection, "INSTALL " + extension);
                execute(connection, "LOAD " + extension);
            } catch (SQLException installFailure) {
                installFailure.addSuppressed(loadFailure);
                throw installFailure;
            }
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void printRows(Statement statement, String table) throws Exception {
        List<String> columns = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery("DESCRIBE SELECT * FROM " + table)) {
            while (resultSet.next()) {
                columns.add(quoteIdentifier(resultSet.getString(1)));
            }
        }

        try (ResultSet resultSet =
                statement.executeQuery(
                        "SELECT "
                                + String.join(", ", columns)
                                + " FROM "
                                + table
                                + " ORDER BY id")) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                List<String> values = new ArrayList<>();
                for (int column = 1; column <= columnCount; column++) {
                    values.add(String.valueOf(resultSet.getObject(column)));
                }
                System.out.println(OUTPUT_PREFIX + String.join(", ", values));
            }
        }
    }

    private static void printColumnType(Statement statement, String table, String columnName)
            throws Exception {
        try (ResultSet resultSet = statement.executeQuery("DESCRIBE SELECT * FROM " + table)) {
            while (resultSet.next()) {
                if (columnName.equals(resultSet.getString(1))) {
                    System.out.println(OUTPUT_PREFIX + resultSet.getString(2));
                    return;
                }
            }
        }
    }

    private static void printFlinkCdcSnapshots(Statement statement) throws Exception {
        try (ResultSet resultSet =
                statement.executeQuery(
                        "SELECT snapshot_id, commit_message, commit_extra_info "
                                + "FROM \"ducklake\".snapshots() WHERE author = 'flink-cdc' "
                                + "ORDER BY snapshot_id")) {
            while (resultSet.next()) {
                System.out.println(
                        OUTPUT_PREFIX
                                + resultSet.getLong(1)
                                + "|"
                                + resultSet.getString(2)
                                + "|"
                                + resultSet.getString(3));
            }
        }
    }

    private static String s3Secret(
            String storagePath,
            String storageEndpoint,
            String storageAccessKey,
            String storageSecretKey) {
        return "CREATE OR REPLACE SECRET flink_cdc_ducklake_e2e_s3 ("
                + "TYPE s3, PROVIDER config, KEY_ID "
                + literal(storageAccessKey)
                + ", SECRET "
                + literal(storageSecretKey)
                + ", REGION 'us-east-1', ENDPOINT "
                + literal(storageEndpoint)
                + ", USE_SSL false, URL_STYLE 'path', SCOPE "
                + literal(storagePath)
                + ")";
    }

    private static String attachStatement(String[] args) {
        String catalog =
                "ducklake:postgres:host="
                        + args[1]
                        + " port="
                        + args[2]
                        + " dbname="
                        + args[3]
                        + " user="
                        + args[4]
                        + " password="
                        + args[5]
                        + " sslmode=disable";
        return "ATTACH "
                + literal(catalog)
                + " AS "
                + quoteIdentifier("ducklake")
                + " (DATA_PATH "
                + literal(args[6])
                + ")";
    }

    private static String qualifiedTable(String database, String table) {
        return quoteIdentifier("ducklake")
                + "."
                + quoteIdentifier(database)
                + "."
                + quoteIdentifier(table);
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
