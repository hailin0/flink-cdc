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

/** Creates local or explicitly shared filesystem storage. */
@Internal
public final class FileSystemStorageProviderFactory implements DuckLakeStorageProviderFactory {

    public static final String TYPE = "filesystem";
    public static final String SHARED_PROPERTY = "shared";

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
                        Collections.singleton(DuckLakeProviderOptions.PATH),
                        Collections.singleton(SHARED_PROPERTY));
        Path path =
                ProviderPathUtils.absoluteLocalPath(
                        DuckLakeDataSinkOptions.PREFIX_STORAGE_PROPERTIES
                                + DuckLakeProviderOptions.PATH,
                        options.required(DuckLakeProviderOptions.PATH));
        options.bool(SHARED_PROPERTY, false);
        return new FileSystemStorageProvider(path);
    }

    private static final class FileSystemStorageProvider implements DuckLakeStorageProvider {

        private static final long serialVersionUID = 1L;

        private final String path;

        private FileSystemStorageProvider(Path path) {
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
                Files.createDirectories(Path.of(path));
            } catch (IOException e) {
                throw new SQLException("Unable to create DuckLake filesystem data path", e);
            }
        }

        @Override
        public String dataPath() {
            return path;
        }

        @Override
        public String toString() {
            return "FileSystemStorageProvider{" + "path='" + path + '\'' + '}';
        }
    }
}
