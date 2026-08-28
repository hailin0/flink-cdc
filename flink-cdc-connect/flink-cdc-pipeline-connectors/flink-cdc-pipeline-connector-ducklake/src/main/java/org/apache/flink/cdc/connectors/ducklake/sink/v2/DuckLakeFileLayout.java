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

package org.apache.flink.cdc.connectors.ducklake.sink.v2;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeCatalogObjects;
import org.apache.flink.cdc.connectors.ducklake.sink.utils.DuckLakeHashUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Builds collision-safe paths for immutable data files and temporary key files. */
final class DuckLakeFileLayout {

    private static final String DATA_DIRECTORY = "flink-cdc";
    private static final String KEY_DIRECTORY = "keys";

    private final String storageRoot;
    private final int subtaskId;
    private final int attemptNumber;
    private final String writerDirectory;

    DuckLakeFileLayout(
            String storageRoot,
            int subtaskId,
            int attemptNumber,
            String sinkId,
            String operatorId) {
        this.storageRoot = normalizeRoot(storageRoot);
        if (subtaskId < 0 || attemptNumber < 0) {
            throw new IllegalArgumentException("subtaskId and attemptNumber must be non-negative");
        }
        this.subtaskId = subtaskId;
        this.attemptNumber = attemptNumber;
        this.writerDirectory = hash(sinkId) + "/" + hash(operatorId);
    }

    String dataFile(
            TableId tableId,
            long writerEpoch,
            int schemaVersion,
            long sequence,
            String materializationId) {
        String schemaName =
                tableId.getSchemaName() == null
                        ? DuckLakeCatalogObjects.DEFAULT_SCHEMA
                        : tableId.getSchemaName();
        return storageRoot
                + "/"
                + encode(schemaName)
                + "/"
                + encode(tableId.getTableName())
                + "/"
                + DATA_DIRECTORY
                + writerPath(writerEpoch, schemaVersion, sequence, materializationId);
    }

    String keyFile(
            TableId tableId,
            long writerEpoch,
            int schemaVersion,
            long sequence,
            String materializationId) {
        return storageRoot
                + "/"
                + DuckLakeCatalogObjects.STAGING_SCHEMA
                + "/"
                + KEY_DIRECTORY
                + "/table-"
                + hash(tableIdentity(tableId))
                + writerPath(writerEpoch, schemaVersion, sequence, materializationId);
    }

    void createParentDirectories(String path) throws IOException {
        if (path.contains("://")) {
            return;
        }
        Path parent = Paths.get(path).getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private String writerPath(
            long writerEpoch, int schemaVersion, long sequence, String materializationId) {
        if (writerEpoch < 0) {
            throw new IllegalArgumentException("writerEpoch must be non-negative");
        }
        return "/"
                + writerDirectory
                + "/writer-epoch-"
                + writerEpoch
                + "/batch-"
                + schemaVersion
                + "/part-"
                + subtaskId
                + "-"
                + attemptNumber
                + "-"
                + sequence
                + "-"
                + Objects.requireNonNull(materializationId, "materializationId must not be null")
                + ".parquet";
    }

    private static String normalizeRoot(String root) {
        Objects.requireNonNull(root, "storageRoot must not be null");
        String normalized = root;
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("storageRoot must not be empty");
        }
        return normalized;
    }

    private static String hash(String value) {
        Objects.requireNonNull(value, "path identity must not be null");
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            return DuckLakeHashUtils.toHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static String encode(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        StringBuilder result = new StringBuilder(bytes.length);
        for (byte current : bytes) {
            int unsigned = current & 0xff;
            if ((unsigned >= 'a' && unsigned <= 'z')
                    || (unsigned >= 'A' && unsigned <= 'Z')
                    || (unsigned >= '0' && unsigned <= '9')
                    || unsigned == '-'
                    || unsigned == '_') {
                result.append((char) unsigned);
            } else {
                result.append('%');
                result.append(Character.toUpperCase(Character.forDigit(unsigned >>> 4, 16)));
                result.append(Character.toUpperCase(Character.forDigit(unsigned & 0x0f, 16)));
            }
        }
        return result.toString();
    }

    private static String tableIdentity(TableId tableId) {
        return String.valueOf(tableId.getNamespace())
                + '\0'
                + String.valueOf(tableId.getSchemaName())
                + '\0'
                + tableId.getTableName();
    }
}
