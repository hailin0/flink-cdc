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

package org.apache.flink.cdc.connectors.ducklake.sink.client;

import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shares one embedded DuckDB client for each local DuckLake catalog in this JVM. */
final class DuckDbSharedCatalogConnectionRegistry {

    private static final Map<String, SharedCatalog> CATALOGS = new HashMap<>();

    private DuckDbSharedCatalogConnectionRegistry() {}

    static Connection acquire(
            String catalogPath, String configurationFingerprint, ConnectionInitializer initializer)
            throws SQLException {
        String catalogKey = canonicalCatalogPath(catalogPath);
        synchronized (CATALOGS) {
            SharedCatalog catalog = CATALOGS.get(catalogKey);
            if (catalog == null) {
                DuckDBConnection root = initializer.openAndInitialize();
                catalog = new SharedCatalog(configurationFingerprint, root);
                CATALOGS.put(catalogKey, catalog);
            } else if (!catalog.configurationFingerprint.equals(configurationFingerprint)) {
                throw new SQLException(
                        "DuckDB catalog "
                                + catalogPath
                                + " is already open with a different connector configuration");
            }

            try {
                DuckDBConnection connection = catalog.root.duplicate();
                catalog.references++;
                return lease(catalogKey, catalog, connection);
            } catch (SQLException failure) {
                if (catalog.references == 0) {
                    CATALOGS.remove(catalogKey);
                    closeAndSuppress(catalog.root, failure);
                }
                throw failure;
            }
        }
    }

    private static Connection lease(
            String catalogPath, SharedCatalog catalog, DuckDBConnection connection) {
        AtomicBoolean released = new AtomicBoolean();
        return (Connection)
                Proxy.newProxyInstance(
                        DuckDbSharedCatalogConnectionRegistry.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            String methodName = method.getName();
                            if ((methodName.equals("close") && method.getParameterCount() == 0)
                                    || (methodName.equals("abort")
                                            && method.getParameterCount() == 1)) {
                                if (released.compareAndSet(false, true)) {
                                    release(catalogPath, catalog, connection);
                                }
                                return null;
                            }
                            if (methodName.equals("isClosed")
                                    && method.getParameterCount() == 0
                                    && released.get()) {
                                return true;
                            }
                            if (methodName.equals("unwrap") && method.getParameterCount() == 1) {
                                Class<?> requestedType = (Class<?>) args[0];
                                if (requestedType.isInstance(proxy)) {
                                    return proxy;
                                }
                                throw new SQLException(
                                        "DuckDB catalog connection lease is not a wrapper for "
                                                + requestedType.getName());
                            }
                            if (methodName.equals("isWrapperFor")
                                    && method.getParameterCount() == 1) {
                                return ((Class<?>) args[0]).isInstance(proxy);
                            }
                            if (method.getDeclaringClass() == Object.class) {
                                switch (methodName) {
                                    case "equals":
                                        return proxy == args[0];
                                    case "hashCode":
                                        return System.identityHashCode(proxy);
                                    case "toString":
                                        return "DuckDbCatalogConnectionLease{" + catalogPath + '}';
                                    default:
                                        break;
                                }
                            }
                            try {
                                return method.invoke(connection, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
    }

    private static void release(
            String catalogPath, SharedCatalog catalog, DuckDBConnection connection)
            throws SQLException {
        synchronized (CATALOGS) {
            SQLException failure = null;
            try {
                connection.close();
            } catch (SQLException e) {
                failure = e;
            } finally {
                catalog.references--;
                if (catalog.references == 0 && CATALOGS.remove(catalogPath, catalog)) {
                    try {
                        catalog.root.close();
                    } catch (SQLException rootFailure) {
                        if (failure == null) {
                            failure = rootFailure;
                        } else {
                            failure.addSuppressed(rootFailure);
                        }
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static String canonicalCatalogPath(String catalogPath) throws SQLException {
        try {
            return Paths.get(catalogPath).toFile().getCanonicalPath();
        } catch (IOException | RuntimeException e) {
            throw new SQLException("Unable to resolve DuckDB catalog path " + catalogPath, e);
        }
    }

    private static void closeAndSuppress(Connection connection, SQLException failure) {
        try {
            connection.close();
        } catch (SQLException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static class SharedCatalog {

        private final String configurationFingerprint;
        private final DuckDBConnection root;
        private int references;

        private SharedCatalog(String configurationFingerprint, DuckDBConnection root) {
            this.configurationFingerprint = configurationFingerprint;
            this.root = root;
        }
    }

    @FunctionalInterface
    interface ConnectionInitializer {
        DuckDBConnection openAndInitialize() throws SQLException;
    }
}
