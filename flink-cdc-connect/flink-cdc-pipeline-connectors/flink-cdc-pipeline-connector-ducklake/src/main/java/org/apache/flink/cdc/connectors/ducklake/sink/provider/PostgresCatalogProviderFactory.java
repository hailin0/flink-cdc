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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.ducklake.sink.DuckLakeDataSinkOptions;

import java.sql.Connection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** Creates the built-in PostgreSQL catalog provider. */
@Internal
public final class PostgresCatalogProviderFactory implements DuckLakeCatalogProviderFactory {

    public static final String TYPE = "postgres";

    private static final Set<String> REQUIRED =
            Set.of(
                    DuckLakeProviderOptions.HOST,
                    DuckLakeProviderOptions.DATABASE,
                    DuckLakeProviderOptions.USER,
                    DuckLakeProviderOptions.PASSWORD);
    private static final Set<String> OPTIONAL =
            Set.of(DuckLakeProviderOptions.PORT, DuckLakeProviderOptions.SSL_MODE);

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DuckLakeCatalogProvider create(Map<String, String> properties) {
        ProviderProperties options =
                new ProviderProperties(
                        DuckLakeDataSinkOptions.PREFIX_CATALOG_PROPERTIES,
                        properties,
                        REQUIRED,
                        OPTIONAL);
        int port = options.integer(DuckLakeProviderOptions.PORT, 5432);
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Option '"
                            + DuckLakeDataSinkOptions.catalogProperty(DuckLakeProviderOptions.PORT)
                            + "' must be between 1 and 65535");
        }
        return new PostgresCatalogProvider(
                options.required(DuckLakeProviderOptions.HOST),
                port,
                options.required(DuckLakeProviderOptions.DATABASE),
                options.required(DuckLakeProviderOptions.USER),
                options.required(DuckLakeProviderOptions.PASSWORD),
                options.optional(DuckLakeProviderOptions.SSL_MODE, "prefer"));
    }

    private static final class PostgresCatalogProvider implements DuckLakeCatalogProvider {

        private static final long serialVersionUID = 1L;

        private final String host;
        private final int port;
        private final String database;
        private final String user;
        private final String password;
        private final String sslMode;

        private PostgresCatalogProvider(
                String host,
                int port,
                String database,
                String user,
                String password,
                String sslMode) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.user = user;
            this.password = password;
            this.sslMode = sslMode;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public Set<DuckDbExtension> requiredExtensions() {
            return Collections.singleton(DuckDbExtension.POSTGRES);
        }

        @Override
        public void configure(Connection connection) {
            // The PostgreSQL parameters are passed through the DuckLake metadata path.
        }

        @Override
        public String metadataPath() {
            return "postgres:host="
                    + postgresValue(host)
                    + " port="
                    + postgresValue(Integer.toString(port))
                    + " dbname="
                    + postgresValue(database)
                    + " user="
                    + postgresValue(user)
                    + " password="
                    + postgresValue(password)
                    + " sslmode="
                    + postgresValue(sslMode);
        }

        private static String postgresValue(String value) {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }

        @Override
        public String toString() {
            return "PostgresCatalogProvider{"
                    + "host='"
                    + host
                    + '\''
                    + ", port="
                    + port
                    + ", database='"
                    + database
                    + '\''
                    + ", user='"
                    + user
                    + '\''
                    + ", sslMode='"
                    + sslMode
                    + '\''
                    + '}';
        }
    }
}
