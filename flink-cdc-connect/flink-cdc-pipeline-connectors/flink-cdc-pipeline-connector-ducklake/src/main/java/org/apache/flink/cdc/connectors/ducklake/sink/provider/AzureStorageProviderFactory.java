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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Creates Azure Blob or ADLSv2 storage through DuckDB's Azure extension. */
@Internal
public final class AzureStorageProviderFactory implements DuckLakeStorageProviderFactory {

    public static final String TYPE = "azure";

    private static final String SECRET_NAME = "flink_cdc_ducklake_azure";
    private static final String CONFIG_PROVIDER = "config";
    private static final String CREDENTIAL_CHAIN_PROVIDER =
            DuckLakeProviderOptions.CREDENTIAL_CHAIN;
    private static final String SERVICE_PRINCIPAL_PROVIDER = "service-principal";
    private static final Set<String> REQUIRED = Set.of(DuckLakeProviderOptions.PATH);
    private static final Set<String> OPTIONAL =
            Set.of(
                    DuckLakeProviderOptions.CREDENTIAL_PROVIDER,
                    DuckLakeProviderOptions.CONNECTION_STRING,
                    DuckLakeProviderOptions.ACCOUNT_NAME,
                    DuckLakeProviderOptions.CREDENTIAL_CHAIN,
                    DuckLakeProviderOptions.TENANT_ID,
                    DuckLakeProviderOptions.CLIENT_ID,
                    DuckLakeProviderOptions.CLIENT_SECRET);

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
                        OPTIONAL);
        String path =
                ProviderPathUtils.remotePath(
                        DuckLakeDataSinkOptions.PREFIX_STORAGE_PROPERTIES
                                + DuckLakeProviderOptions.PATH,
                        options.required(DuckLakeProviderOptions.PATH),
                        "az",
                        "azure",
                        "abfss");
        String credentialProvider =
                options.optional(DuckLakeProviderOptions.CREDENTIAL_PROVIDER, CONFIG_PROVIDER)
                        .trim()
                        .toLowerCase(Locale.ROOT);
        validateCredentials(options, credentialProvider);
        return new AzureStorageProvider(
                path,
                credentialProvider,
                options.optional(DuckLakeProviderOptions.CONNECTION_STRING),
                options.optional(DuckLakeProviderOptions.ACCOUNT_NAME),
                options.optional(DuckLakeProviderOptions.CREDENTIAL_CHAIN),
                options.optional(DuckLakeProviderOptions.TENANT_ID),
                options.optional(DuckLakeProviderOptions.CLIENT_ID),
                options.optional(DuckLakeProviderOptions.CLIENT_SECRET));
    }

    private static void validateCredentials(ProviderProperties options, String credentialProvider) {
        switch (credentialProvider) {
            case CONFIG_PROVIDER:
                options.required(DuckLakeProviderOptions.CONNECTION_STRING);
                rejectPresent(
                        options,
                        "config",
                        DuckLakeProviderOptions.ACCOUNT_NAME,
                        DuckLakeProviderOptions.CREDENTIAL_CHAIN,
                        DuckLakeProviderOptions.TENANT_ID,
                        DuckLakeProviderOptions.CLIENT_ID,
                        DuckLakeProviderOptions.CLIENT_SECRET);
                break;
            case CREDENTIAL_CHAIN_PROVIDER:
                options.required(DuckLakeProviderOptions.ACCOUNT_NAME);
                rejectPresent(
                        options,
                        DuckLakeProviderOptions.CREDENTIAL_CHAIN,
                        DuckLakeProviderOptions.CONNECTION_STRING,
                        DuckLakeProviderOptions.TENANT_ID,
                        DuckLakeProviderOptions.CLIENT_ID,
                        DuckLakeProviderOptions.CLIENT_SECRET);
                break;
            case SERVICE_PRINCIPAL_PROVIDER:
                options.required(DuckLakeProviderOptions.ACCOUNT_NAME);
                options.required(DuckLakeProviderOptions.TENANT_ID);
                options.required(DuckLakeProviderOptions.CLIENT_ID);
                options.required(DuckLakeProviderOptions.CLIENT_SECRET);
                rejectPresent(
                        options,
                        "service-principal",
                        DuckLakeProviderOptions.CONNECTION_STRING,
                        DuckLakeProviderOptions.CREDENTIAL_CHAIN);
                break;
            default:
                throw new IllegalArgumentException(
                        DuckLakeDataSinkOptions.storageProperty(
                                        DuckLakeProviderOptions.CREDENTIAL_PROVIDER)
                                + " must be 'config', "
                                + "'credential-chain', or 'service-principal'");
        }
    }

    private static void rejectPresent(ProviderProperties options, String provider, String... keys) {
        for (String key : keys) {
            if (options.optional(key) != null) {
                throw new IllegalArgumentException(
                        DuckLakeDataSinkOptions.storageProperty(key)
                                + " cannot be used with "
                                + DuckLakeDataSinkOptions.storageProperty(
                                        DuckLakeProviderOptions.CREDENTIAL_PROVIDER)
                                + "="
                                + provider);
            }
        }
    }

    private static final class AzureStorageProvider implements DuckLakeStorageProvider {

        private static final long serialVersionUID = 1L;

        private final String path;
        private final String credentialProvider;
        private final String connectionString;
        private final String accountName;
        private final String credentialChain;
        private final String tenantId;
        private final String clientId;
        private final String clientSecret;

        private AzureStorageProvider(
                String path,
                String credentialProvider,
                String connectionString,
                String accountName,
                String credentialChain,
                String tenantId,
                String clientId,
                String clientSecret) {
            this.path = path;
            this.credentialProvider = credentialProvider;
            this.connectionString = connectionString;
            this.accountName = accountName;
            this.credentialChain = credentialChain;
            this.tenantId = tenantId;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public Set<DuckDbExtension> requiredExtensions() {
            return Set.of(DuckDbExtension.AZURE);
        }

        @Override
        public void configure(Connection connection) throws SQLException {
            List<String> parameters = new ArrayList<>();
            parameters.add("TYPE azure");
            parameters.add(
                    "PROVIDER " + credentialProvider.replace('-', '_').toLowerCase(Locale.ROOT));
            switch (credentialProvider) {
                case CONFIG_PROVIDER:
                    parameters.add("CONNECTION_STRING " + DuckDbSqlUtils.literal(connectionString));
                    break;
                case CREDENTIAL_CHAIN_PROVIDER:
                    parameters.add("ACCOUNT_NAME " + DuckDbSqlUtils.literal(accountName));
                    if (credentialChain != null) {
                        parameters.add("CHAIN " + DuckDbSqlUtils.literal(credentialChain));
                    }
                    break;
                case SERVICE_PRINCIPAL_PROVIDER:
                    parameters.add("TENANT_ID " + DuckDbSqlUtils.literal(tenantId));
                    parameters.add("CLIENT_ID " + DuckDbSqlUtils.literal(clientId));
                    parameters.add("CLIENT_SECRET " + DuckDbSqlUtils.literal(clientSecret));
                    parameters.add("ACCOUNT_NAME " + DuckDbSqlUtils.literal(accountName));
                    break;
                default:
                    throw new IllegalStateException(
                            "Unsupported Azure credential provider " + credentialProvider);
            }
            parameters.add(
                    "SCOPE " + DuckDbSqlUtils.literal(ProviderPathUtils.withTrailingSlash(path)));
            DuckDbSecretUtils.createOrReplace(connection, SECRET_NAME, parameters);
        }

        @Override
        public String dataPath() {
            return path;
        }

        @Override
        public String toString() {
            return "AzureStorageProvider{"
                    + "path='"
                    + path
                    + '\''
                    + ", credentialProvider='"
                    + credentialProvider
                    + '\''
                    + ", accountName='"
                    + accountName
                    + '\''
                    + '}';
        }
    }
}
