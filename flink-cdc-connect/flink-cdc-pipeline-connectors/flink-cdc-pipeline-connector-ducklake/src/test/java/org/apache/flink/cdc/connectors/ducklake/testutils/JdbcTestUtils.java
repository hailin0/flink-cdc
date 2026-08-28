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

package org.apache.flink.cdc.connectors.ducklake.testutils;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/** JDBC test doubles shared by tests that only need to record executed statements. */
public final class JdbcTestUtils {

    private JdbcTestUtils() {}

    public static Connection recordingConnection(List<String> statements) {
        Statement statement =
                (Statement)
                        Proxy.newProxyInstance(
                                JdbcTestUtils.class.getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("execute")) {
                                        statements.add((String) args[0]);
                                        return true;
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        return (Connection)
                Proxy.newProxyInstance(
                        JdbcTestUtils.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("createStatement")) {
                                return statement;
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    public static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
