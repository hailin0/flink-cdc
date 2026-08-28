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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** Creates the single-client DuckDB file catalog provider. */
@Internal
public final class DuckDbCatalogProviderFactory implements DuckLakeCatalogProviderFactory {

    public static final String TYPE = "duckdb";

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
                        Collections.singleton(DuckLakeProviderOptions.PATH),
                        Collections.emptySet());
        return new DuckDbCatalogProvider(
                ProviderPathUtils.absoluteLocalPath(
                        DuckLakeDataSinkOptions.PREFIX_CATALOG_PROPERTIES
                                + DuckLakeProviderOptions.PATH,
                        options.required(DuckLakeProviderOptions.PATH)));
    }

    private static final class DuckDbCatalogProvider implements DuckLakeCatalogProvider {

        private static final long serialVersionUID = 1L;

        private final String path;

        private DuckDbCatalogProvider(Path path) {
            this.path = path.toString();
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public Set<DuckDbExtension> requiredExtensions() {
            return Collections.emptySet();
        }

        @Override
        public void configure(Connection connection) throws SQLException {
            try {
                Files.createDirectories(Path.of(path).getParent());
            } catch (IOException e) {
                throw new SQLException("Unable to create DuckDB catalog directory", e);
            }
        }

        @Override
        public String metadataPath() {
            return path;
        }

        @Override
        public String toString() {
            return "DuckDbCatalogProvider{" + "path='" + path + '\'' + '}';
        }
    }
}
