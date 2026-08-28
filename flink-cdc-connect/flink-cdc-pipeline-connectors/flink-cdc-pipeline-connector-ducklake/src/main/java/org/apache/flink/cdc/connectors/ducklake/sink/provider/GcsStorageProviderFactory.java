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
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckDbSqlUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Creates Google Cloud Storage backed by DuckDB's GCS HMAC secret. */
@Internal
public final class GcsStorageProviderFactory implements DuckLakeStorageProviderFactory {

    public static final String TYPE = "gcs";

    private static final String SECRET_NAME = "flink_cdc_ducklake_gcs";
    private static final Set<String> REQUIRED =
            Set.of(
                    DuckLakeProviderOptions.PATH,
                    DuckLakeProviderOptions.ACCESS_KEY,
                    DuckLakeProviderOptions.SECRET_KEY);

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DuckLakeStorageProvider create(Map<String, String> properties) {
        ProviderProperties options =
                new ProviderProperties(
                        DuckLakeDataSinkOptions.PREFIX_STORAGE_PROPERTIES,
                        properties,
                        REQUIRED,
                        Collections.emptySet());
        String path =
                ProviderPathUtils.remotePath(
                        DuckLakeDataSinkOptions.PREFIX_STORAGE_PROPERTIES
                                + DuckLakeProviderOptions.PATH,
                        options.required(DuckLakeProviderOptions.PATH),
                        "gcs",
                        "gs");
        return new GcsStorageProvider(
                path,
                options.required(DuckLakeProviderOptions.ACCESS_KEY),
                options.required(DuckLakeProviderOptions.SECRET_KEY));
    }

    private static final class GcsStorageProvider implements DuckLakeStorageProvider {

        private static final long serialVersionUID = 1L;

        private final String path;
        private final String accessKey;
        private final String secretKey;

        private GcsStorageProvider(String path, String accessKey, String secretKey) {
            this.path = path;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public Set<DuckDbExtension> requiredExtensions() {
            return Collections.singleton(DuckDbExtension.HTTPFS);
        }

        @Override
        public void configure(Connection connection) throws SQLException {
            List<String> parameters =
                    Arrays.asList(
                            "TYPE gcs",
                            "PROVIDER config",
                            "KEY_ID " + DuckDbSqlUtils.literal(accessKey),
                            "SECRET " + DuckDbSqlUtils.literal(secretKey),
                            "SCOPE " + DuckDbSqlUtils.literal(path));
            DuckDbSecretUtils.createOrReplace(connection, SECRET_NAME, parameters);
        }

        @Override
        public String dataPath() {
            return path;
        }

        @Override
        public String toString() {
            return "GcsStorageProvider{" + "path='" + path + '\'' + '}';
        }
    }
}
