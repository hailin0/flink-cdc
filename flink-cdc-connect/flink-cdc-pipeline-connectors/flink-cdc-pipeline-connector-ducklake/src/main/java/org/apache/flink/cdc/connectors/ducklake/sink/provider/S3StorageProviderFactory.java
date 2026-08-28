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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Creates the built-in S3-compatible storage provider. */
@Internal
public final class S3StorageProviderFactory implements DuckLakeStorageProviderFactory {

    public static final String TYPE = "s3";

    private static final String SECRET_NAME = "flink_cdc_ducklake_s3";
    private static final String CONFIG_PROVIDER = "config";
    private static final String CREDENTIAL_CHAIN_PROVIDER =
            DuckLakeProviderOptions.CREDENTIAL_CHAIN;
    private static final Set<String> REQUIRED = Collections.singleton(DuckLakeProviderOptions.PATH);
    private static final Set<String> OPTIONAL =
            Set.of(
                    DuckLakeProviderOptions.ENDPOINT,
                    DuckLakeProviderOptions.REGION,
                    DuckLakeProviderOptions.ACCESS_KEY,
                    DuckLakeProviderOptions.SECRET_KEY,
                    DuckLakeProviderOptions.SESSION_TOKEN,
                    DuckLakeProviderOptions.PATH_STYLE_ACCESS,
                    DuckLakeProviderOptions.CREDENTIAL_PROVIDER,
                    DuckLakeProviderOptions.CREDENTIAL_CHAIN,
                    DuckLakeProviderOptions.PROFILE);

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
        String path = options.required(DuckLakeProviderOptions.PATH);
        validateStoragePath(path);
        String credentialProvider =
                options.optional(DuckLakeProviderOptions.CREDENTIAL_PROVIDER, CONFIG_PROVIDER)
                        .trim()
                        .toLowerCase(Locale.ROOT);
        if (!CONFIG_PROVIDER.equals(credentialProvider)
                && !CREDENTIAL_CHAIN_PROVIDER.equals(credentialProvider)) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.storageProperty(
                                    DuckLakeProviderOptions.CREDENTIAL_PROVIDER)
                            + " must be 'config' or "
                            + "'credential-chain'");
        }
        String accessKey = options.optional(DuckLakeProviderOptions.ACCESS_KEY);
        String secretKey = options.optional(DuckLakeProviderOptions.SECRET_KEY);
        String sessionToken = options.optional(DuckLakeProviderOptions.SESSION_TOKEN);
        String credentialChain = options.optional(DuckLakeProviderOptions.CREDENTIAL_CHAIN);
        String profile = options.optional(DuckLakeProviderOptions.PROFILE);
        String endpoint = options.optional(DuckLakeProviderOptions.ENDPOINT);
        if (endpoint != null) {
            Endpoint.parse(endpoint);
        }
        if (CONFIG_PROVIDER.equals(credentialProvider)) {
            accessKey = options.required(DuckLakeProviderOptions.ACCESS_KEY);
            secretKey = options.required(DuckLakeProviderOptions.SECRET_KEY);
            if (credentialChain != null || profile != null) {
                throw new IllegalArgumentException(
                        DuckLakeDataSinkOptions.storageProperty(
                                        DuckLakeProviderOptions.CREDENTIAL_CHAIN)
                                + " and "
                                + DuckLakeDataSinkOptions.storageProperty(
                                        DuckLakeProviderOptions.PROFILE)
                                + " require "
                                + DuckLakeDataSinkOptions.storageProperty(
                                        DuckLakeProviderOptions.CREDENTIAL_PROVIDER)
                                + "=credential-chain");
            }
        } else if (accessKey != null || secretKey != null || sessionToken != null) {
            throw new IllegalArgumentException(
                    "Static S3 credentials cannot be combined with "
                            + DuckLakeDataSinkOptions.storageProperty(
                                    DuckLakeProviderOptions.CREDENTIAL_PROVIDER)
                            + "="
                            + DuckLakeProviderOptions.CREDENTIAL_CHAIN);
        }
        return new S3StorageProvider(
                path,
                endpoint,
                options.optional(
                        DuckLakeProviderOptions.REGION,
                        CONFIG_PROVIDER.equals(credentialProvider) ? "us-east-1" : null),
                credentialProvider,
                accessKey,
                secretKey,
                sessionToken,
                credentialChain,
                profile,
                options.bool(DuckLakeProviderOptions.PATH_STYLE_ACCESS, false));
    }

    private static void validateStoragePath(String path) {
        final URI uri;
        try {
            uri = new URI(path);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.PATH)
                            + " must be a valid S3 URI",
                    e);
        }
        if (!"s3".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.PATH)
                            + " must use s3://");
        }
        if (uri.getRawAuthority() == null || uri.getRawAuthority().isEmpty()) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.PATH)
                            + " must contain an S3 bucket");
        }
        if (uri.getRawUserInfo() != null
                || uri.getPort() != -1
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null) {
            throw new IllegalArgumentException(
                    DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.PATH)
                            + " must be an S3 URI without credentials, port, query, or fragment");
        }
    }

    private static final class S3StorageProvider implements DuckLakeStorageProvider {

        private static final long serialVersionUID = 1L;

        private final String path;
        private final String endpoint;
        private final String region;
        private final String credentialProvider;
        private final String accessKey;
        private final String secretKey;
        private final String sessionToken;
        private final String credentialChain;
        private final String profile;
        private final boolean pathStyleAccess;

        private S3StorageProvider(
                String path,
                String endpoint,
                String region,
                String credentialProvider,
                String accessKey,
                String secretKey,
                String sessionToken,
                String credentialChain,
                String profile,
                boolean pathStyleAccess) {
            this.path = path;
            this.endpoint = endpoint;
            this.region = region;
            this.credentialProvider = credentialProvider;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.sessionToken = sessionToken;
            this.credentialChain = credentialChain;
            this.profile = profile;
            this.pathStyleAccess = pathStyleAccess;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public Set<DuckDbExtension> requiredExtensions() {
            Set<DuckDbExtension> extensions = new LinkedHashSet<>();
            extensions.add(DuckDbExtension.HTTPFS);
            if (CREDENTIAL_CHAIN_PROVIDER.equals(credentialProvider)) {
                extensions.add(DuckDbExtension.AWS);
            }
            return extensions;
        }

        @Override
        public void configure(Connection connection) throws SQLException {
            List<String> parameters = new ArrayList<>();
            parameters.add("TYPE s3");
            if (CONFIG_PROVIDER.equals(credentialProvider)) {
                parameters.add("PROVIDER config");
                parameters.add("KEY_ID " + DuckDbSqlUtils.literal(accessKey));
                parameters.add("SECRET " + DuckDbSqlUtils.literal(secretKey));
                if (sessionToken != null) {
                    parameters.add("SESSION_TOKEN " + DuckDbSqlUtils.literal(sessionToken));
                }
            } else {
                parameters.add("PROVIDER credential_chain");
                if (credentialChain != null) {
                    parameters.add("CHAIN " + DuckDbSqlUtils.literal(credentialChain));
                }
                if (profile != null) {
                    parameters.add("PROFILE " + DuckDbSqlUtils.literal(profile));
                }
                parameters.add("REFRESH auto");
            }
            if (region != null) {
                parameters.add("REGION " + DuckDbSqlUtils.literal(region));
            }
            if (endpoint != null) {
                Endpoint parsedEndpoint = Endpoint.parse(endpoint);
                parameters.add("ENDPOINT " + DuckDbSqlUtils.literal(parsedEndpoint.authority));
                parameters.add("USE_SSL " + parsedEndpoint.useSsl);
            }
            parameters.add(
                    "URL_STYLE " + DuckDbSqlUtils.literal(pathStyleAccess ? "path" : "vhost"));
            parameters.add("SCOPE " + DuckDbSqlUtils.literal(path));
            DuckDbSecretUtils.createOrReplace(connection, SECRET_NAME, parameters);
        }

        @Override
        public String dataPath() {
            return path;
        }

        @Override
        public String toString() {
            return "S3StorageProvider{"
                    + "path='"
                    + path
                    + '\''
                    + ", endpoint='"
                    + endpoint
                    + '\''
                    + ", region='"
                    + region
                    + '\''
                    + ", credentialProvider='"
                    + credentialProvider
                    + '\''
                    + ", pathStyleAccess="
                    + pathStyleAccess
                    + '}';
        }
    }

    private static final class Endpoint {
        private final String authority;
        private final boolean useSsl;

        private Endpoint(String authority, boolean useSsl) {
            this.authority = authority;
            this.useSsl = useSsl;
        }

        private static Endpoint parse(String endpoint) {
            String value = endpoint.trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(
                        DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.ENDPOINT)
                                + " must not be blank");
            }
            String candidate = value.contains("://") ? value : "https://" + value;
            final URI uri;
            try {
                uri = new URI(candidate);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(
                        DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.ENDPOINT)
                                + " must be a valid HTTP(S) endpoint",
                        e);
            }
            if (!"http".equalsIgnoreCase(uri.getScheme())
                    && !"https".equalsIgnoreCase(uri.getScheme())) {
                throw new IllegalArgumentException(
                        DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.ENDPOINT)
                                + " must use http:// or https://");
            }
            if (uri.getHost() == null || uri.getRawUserInfo() != null) {
                throw new IllegalArgumentException(
                        DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.ENDPOINT)
                                + " must contain only a host and optional port");
            }
            if ((uri.getRawPath() != null
                            && !uri.getRawPath().isEmpty()
                            && !"/".equals(uri.getRawPath()))
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null) {
                throw new IllegalArgumentException(
                        DuckLakeDataSinkOptions.storageProperty(DuckLakeProviderOptions.ENDPOINT)
                                + " must not contain a path, query, or fragment");
            }
            return new Endpoint(uri.getRawAuthority(), "https".equalsIgnoreCase(uri.getScheme()));
        }
    }
}
