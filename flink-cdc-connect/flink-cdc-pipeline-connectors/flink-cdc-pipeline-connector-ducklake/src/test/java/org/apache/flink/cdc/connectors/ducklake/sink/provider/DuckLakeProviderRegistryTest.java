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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.ducklake.testutils.JdbcTestUtils.recordingConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DuckLakeProviderRegistry}. */
class DuckLakeProviderRegistryTest {

    @TempDir Path tempDir;

    @Test
    void createsLocalCatalogAndStorageDirectories() throws Exception {
        Path catalogPath = tempDir.resolve("catalog/metadata.ducklake");
        Path storagePath = tempDir.resolve("warehouse");
        DuckLakeCatalogProvider catalog =
                DuckLakeProviderRegistry.createCatalog(
                        "duckdb", Map.of("path", catalogPath.toString()));
        DuckLakeStorageProvider storage =
                DuckLakeProviderRegistry.createStorage(
                        "filesystem", Map.of("path", storagePath.toString(), "shared", "false"));

        try (Connection connection = recordingConnection(new ArrayList<>())) {
            catalog.configure(connection);
            storage.configure(connection);
        }

        assertThat(catalog.requiredExtensions()).isEmpty();
        assertThat(storage.requiredExtensions()).isEmpty();
        assertThat(catalog.metadataPath()).isEqualTo(catalogPath.toString());
        assertThat(storage.dataPath()).isEqualTo(storagePath.toString());
        assertThat(catalogPath.getParent()).isDirectory();
        assertThat(storagePath).isDirectory();
    }

    @Test
    void rejectsRelativeLocalPaths() {
        assertThatThrownBy(
                        () ->
                                DuckLakeProviderRegistry.createCatalog(
                                        "duckdb", Map.of("path", "catalog.ducklake")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("catalog.properties.path must be an absolute path");
        assertThatThrownBy(
                        () ->
                                DuckLakeProviderRegistry.createStorage(
                                        "filesystem", Map.of("path", "warehouse")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storage.properties.path must be an absolute path");
    }

    @Test
    void rejectsFilesystemRootPaths() {
        assertThatThrownBy(
                        () ->
                                DuckLakeProviderRegistry.createCatalog(
                                        "duckdb", Map.of("path", tempDir.getRoot().toString())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("catalog.properties.path must not be the filesystem root");
        assertThatThrownBy(
                        () ->
                                DuckLakeProviderRegistry.createStorage(
                                        "filesystem", Map.of("path", tempDir.getRoot().toString())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storage.properties.path must not be the filesystem root");
    }

    @Test
    void configuresGcsHmacSecret() throws Exception {
        DuckLakeStorageProvider storage =
                DuckLakeProviderRegistry.createStorage(
                        "gcs",
                        Map.of(
                                "path", "gcs://warehouse/ducklake",
                                "access-key", "gcs-access",
                                "secret-key", "gcs-secret"));
        List<String> statements = new ArrayList<>();

        try (Connection connection = recordingConnection(statements)) {
            storage.configure(connection);
        }

        assertThat(storage.requiredExtensions()).containsExactly(DuckDbExtension.HTTPFS);
        assertThat(statements)
                .containsExactly(
                        "CREATE OR REPLACE SECRET \"flink_cdc_ducklake_gcs\" "
                                + "(TYPE gcs, PROVIDER config, KEY_ID 'gcs-access', "
                                + "SECRET 'gcs-secret', SCOPE 'gcs://warehouse/ducklake')");
    }

    @Test
    void configuresR2Secret() throws Exception {
        DuckLakeStorageProvider storage =
                DuckLakeProviderRegistry.createStorage(
                        "r2",
                        Map.of(
                                "path", "r2://warehouse/ducklake",
                                "account-id", "account",
                                "access-key", "r2-access",
                                "secret-key", "r2-secret"));
        List<String> statements = new ArrayList<>();

        try (Connection connection = recordingConnection(statements)) {
            storage.configure(connection);
        }

        assertThat(storage.requiredExtensions()).containsExactly(DuckDbExtension.HTTPFS);
        assertThat(statements)
                .containsExactly(
                        "CREATE OR REPLACE SECRET \"flink_cdc_ducklake_r2\" "
                                + "(TYPE r2, PROVIDER config, KEY_ID 'r2-access', "
                                + "SECRET 'r2-secret', ACCOUNT_ID 'account', "
                                + "SCOPE 'r2://warehouse/ducklake')");
    }

    @Test
    void configuresAzureConnectionStringSecret() throws Exception {
        DuckLakeStorageProvider storage =
                DuckLakeProviderRegistry.createStorage(
                        "azure",
                        Map.of(
                                "path", "az://warehouse/ducklake",
                                "credential-provider", "config",
                                "connection-string", "UseDevelopmentStorage=true"));
        List<String> statements = new ArrayList<>();

        try (Connection connection = recordingConnection(statements)) {
            storage.configure(connection);
        }

        assertThat(storage.requiredExtensions()).containsExactly(DuckDbExtension.AZURE);
        assertThat(statements)
                .containsExactly(
                        "CREATE OR REPLACE SECRET \"flink_cdc_ducklake_azure\" "
                                + "(TYPE azure, PROVIDER config, "
                                + "CONNECTION_STRING 'UseDevelopmentStorage=true', "
                                + "SCOPE 'az://warehouse/ducklake/')");
    }

    @Test
    void configuresAzureCredentialChain() throws Exception {
        DuckLakeStorageProvider storage =
                DuckLakeProviderRegistry.createStorage(
                        "azure",
                        Map.of(
                                "path", "abfss://warehouse/ducklake",
                                "credential-provider", "credential-chain",
                                "account-name", "account",
                                "credential-chain", "managed_identity;env"));
        List<String> statements = new ArrayList<>();

        try (Connection connection = recordingConnection(statements)) {
            storage.configure(connection);
        }

        assertThat(statements.get(0))
                .contains(
                        "PROVIDER credential_chain",
                        "ACCOUNT_NAME 'account'",
                        "CHAIN 'managed_identity;env'",
                        "SCOPE 'abfss://warehouse/ducklake/'");
    }

    @Test
    void acceptsFullyQualifiedAzurePaths() {
        DuckLakeStorageProvider blobStorage =
                DuckLakeProviderRegistry.createStorage(
                        "azure",
                        Map.of(
                                "path",
                                "az://account.blob.core.windows.net/warehouse/ducklake",
                                "credential-provider",
                                "credential-chain",
                                "account-name",
                                "account"));
        DuckLakeStorageProvider adlsStorage =
                DuckLakeProviderRegistry.createStorage(
                        "azure",
                        Map.of(
                                "path",
                                "abfss://account.dfs.core.windows.net/warehouse/ducklake",
                                "credential-provider",
                                "credential-chain",
                                "account-name",
                                "account"));

        assertThat(blobStorage.dataPath())
                .isEqualTo("az://account.blob.core.windows.net/warehouse/ducklake");
        assertThat(adlsStorage.dataPath())
                .isEqualTo("abfss://account.dfs.core.windows.net/warehouse/ducklake");
    }

    @Test
    void rejectsAccountNameWithAzureConnectionString() {
        assertThatThrownBy(
                        () ->
                                DuckLakeProviderRegistry.createStorage(
                                        "azure",
                                        Map.of(
                                                "path",
                                                "az://warehouse/ducklake",
                                                "connection-string",
                                                "connection",
                                                "account-name",
                                                "account")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "storage.properties.account-name cannot be used with "
                                + "storage.properties.credential-provider=config");
    }

    @Test
    void requiresCompleteAzureServicePrincipalCredentials() {
        assertThatThrownBy(
                        () ->
                                DuckLakeProviderRegistry.createStorage(
                                        "azure",
                                        Map.of(
                                                "path",
                                                "az://warehouse/ducklake",
                                                "credential-provider",
                                                "service-principal",
                                                "account-name",
                                                "account",
                                                "tenant-id",
                                                "tenant",
                                                "client-id",
                                                "client")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storage.properties.client-secret");
    }

    @Test
    void masksCloudCredentialsFromDiagnosticStrings() {
        DuckLakeStorageProvider gcs =
                DuckLakeProviderRegistry.createStorage(
                        "gcs",
                        Map.of(
                                "path", "gcs://warehouse/ducklake",
                                "access-key", "gcs-access",
                                "secret-key", "gcs-secret"));
        DuckLakeStorageProvider r2 =
                DuckLakeProviderRegistry.createStorage(
                        "r2",
                        Map.of(
                                "path", "r2://warehouse/ducklake",
                                "account-id", "account",
                                "access-key", "r2-access",
                                "secret-key", "r2-secret"));
        DuckLakeStorageProvider azure =
                DuckLakeProviderRegistry.createStorage(
                        "azure",
                        Map.of(
                                "path", "az://warehouse/ducklake",
                                "connection-string", "azure-secret"));

        assertThat(gcs.toString()).doesNotContain("gcs-access", "gcs-secret");
        assertThat(r2.toString()).doesNotContain("r2-access", "r2-secret");
        assertThat(azure.toString()).doesNotContain("azure-secret");
    }

    @Test
    void configuresAzureServicePrincipal() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "azure://warehouse/ducklake");
        properties.put("credential-provider", "service-principal");
        properties.put("account-name", "account");
        properties.put("tenant-id", "tenant");
        properties.put("client-id", "client");
        properties.put("client-secret", "client-secret");
        DuckLakeStorageProvider storage =
                DuckLakeProviderRegistry.createStorage("azure", properties);
        List<String> statements = new ArrayList<>();

        try (Connection connection = recordingConnection(statements)) {
            storage.configure(connection);
        }

        assertThat(statements.get(0))
                .contains(
                        "PROVIDER service_principal",
                        "ACCOUNT_NAME 'account'",
                        "TENANT_ID 'tenant'",
                        "CLIENT_ID 'client'",
                        "CLIENT_SECRET 'client-secret'",
                        "SCOPE 'azure://warehouse/ducklake/'");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storagePropertiesWithWrongScheme")
    void rejectsCloudPathWithWrongScheme(
            String testCase,
            String storageType,
            Map<String, String> properties,
            String expectedMessage) {
        assertThatThrownBy(() -> DuckLakeProviderRegistry.createStorage(storageType, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedMessage);
    }

    private static Stream<Arguments> storagePropertiesWithWrongScheme() {
        return Stream.of(
                Arguments.of(
                        "GCS requires a GCS URI",
                        "gcs",
                        Map.of(
                                "path", "s3://warehouse/ducklake",
                                "access-key", "access",
                                "secret-key", "secret"),
                        "must use gcs:// or gs://"),
                Arguments.of(
                        "R2 requires an R2 URI",
                        "r2",
                        Map.of(
                                "path", "s3://warehouse/ducklake",
                                "account-id", "account",
                                "access-key", "access",
                                "secret-key", "secret"),
                        "must use r2://"),
                Arguments.of(
                        "Azure requires an Azure URI",
                        "azure",
                        Map.of(
                                "path", "s3://warehouse/ducklake",
                                "credential-provider", "config",
                                "connection-string", "connection"),
                        "must use az://, azure://, or abfss://"));
    }
}
