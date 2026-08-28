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

/** Property names shared by built-in DuckLake providers. */
final class DuckLakeProviderOptions {

    static final String ACCESS_KEY = "access-key";
    static final String ACCOUNT_ID = "account-id";
    static final String ACCOUNT_NAME = "account-name";
    static final String CLIENT_ID = "client-id";
    static final String CLIENT_SECRET = "client-secret";
    static final String CONNECTION_STRING = "connection-string";
    static final String CREDENTIAL_CHAIN = "credential-chain";
    static final String CREDENTIAL_PROVIDER = "credential-provider";
    static final String DATABASE = "database";
    static final String ENDPOINT = "endpoint";
    static final String HOST = "host";
    static final String PASSWORD = "password";
    static final String PATH = "path";
    static final String PATH_STYLE_ACCESS = "path-style-access";
    static final String PORT = "port";
    static final String PROFILE = "profile";
    static final String REGION = "region";
    static final String SECRET_KEY = "secret-key";
    static final String SESSION_TOKEN = "session-token";
    static final String SSL_MODE = "ssl-mode";
    static final String TENANT_ID = "tenant-id";
    static final String USER = "user";

    private DuckLakeProviderOptions() {}
}
