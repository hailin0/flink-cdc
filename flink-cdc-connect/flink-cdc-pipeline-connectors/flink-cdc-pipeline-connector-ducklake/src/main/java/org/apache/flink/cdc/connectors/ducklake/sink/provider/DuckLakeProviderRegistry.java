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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Registry of providers bundled in the DuckLake connector. */
@Internal
public final class DuckLakeProviderRegistry {

    private static final Map<String, DuckLakeCatalogProviderFactory> CATALOG_FACTORIES =
            catalogFactories(
                    new PostgresCatalogProviderFactory(), new DuckDbCatalogProviderFactory());
    private static final Map<String, DuckLakeStorageProviderFactory> STORAGE_FACTORIES =
            storageFactories(
                    new S3StorageProviderFactory(),
                    new FileSystemStorageProviderFactory(),
                    new GcsStorageProviderFactory(),
                    new R2StorageProviderFactory(),
                    new AzureStorageProviderFactory());

    private DuckLakeProviderRegistry() {}

    public static DuckLakeCatalogProvider createCatalog(
            String type, Map<String, String> properties) {
        String normalizedType = normalize(type);
        DuckLakeCatalogProviderFactory factory = CATALOG_FACTORIES.get(normalizedType);
        if (factory == null) {
            throw new IllegalArgumentException(
                    "Unsupported "
                            + DuckLakeDataSinkOptions.CATALOG_TYPE.key()
                            + " '"
                            + type
                            + "'. Supported types are "
                            + CATALOG_FACTORIES.keySet());
        }
        return factory.create(properties);
    }

    public static DuckLakeStorageProvider createStorage(
            String type, Map<String, String> properties) {
        String normalizedType = normalize(type);
        DuckLakeStorageProviderFactory factory = STORAGE_FACTORIES.get(normalizedType);
        if (factory == null) {
            throw new IllegalArgumentException(
                    "Unsupported "
                            + DuckLakeDataSinkOptions.STORAGE_TYPE.key()
                            + " '"
                            + type
                            + "'. Supported types are "
                            + STORAGE_FACTORIES.keySet());
        }
        return factory.create(properties);
    }

    private static String normalize(String type) {
        return type.trim().toLowerCase(Locale.ROOT);
    }

    private static Map<String, DuckLakeCatalogProviderFactory> catalogFactories(
            DuckLakeCatalogProviderFactory... factories) {
        Map<String, DuckLakeCatalogProviderFactory> result = new LinkedHashMap<>();
        Arrays.stream(factories)
                .forEach(
                        factory -> register(result, normalize(factory.type()), factory, "catalog"));
        return result;
    }

    private static Map<String, DuckLakeStorageProviderFactory> storageFactories(
            DuckLakeStorageProviderFactory... factories) {
        Map<String, DuckLakeStorageProviderFactory> result = new LinkedHashMap<>();
        Arrays.stream(factories)
                .forEach(
                        factory -> register(result, normalize(factory.type()), factory, "storage"));
        return result;
    }

    private static <T> void register(
            Map<String, T> factories, String type, T factory, String providerKind) {
        if (factories.putIfAbsent(type, factory) != null) {
            throw new IllegalStateException(
                    "Duplicate built-in DuckLake "
                            + providerKind
                            + " provider type '"
                            + type
                            + "'");
        }
    }
}
