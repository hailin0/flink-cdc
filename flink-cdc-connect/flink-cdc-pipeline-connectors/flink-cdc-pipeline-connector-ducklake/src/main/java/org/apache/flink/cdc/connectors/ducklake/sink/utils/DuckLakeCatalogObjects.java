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

package org.apache.flink.cdc.connectors.ducklake.sink.utils;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;

import javax.annotation.Nullable;

import java.util.Locale;

/** Catalog objects and lock identifiers exclusively owned by the DuckLake connector. */
@Internal
public final class DuckLakeCatalogObjects {

    public static final String CATALOG_ALIAS = "ducklake";
    public static final String DEFAULT_SCHEMA = "main";
    public static final String INTERNAL_SCHEMA = "_flink_cdc_internal";
    public static final String STAGING_SCHEMA = "_flink_cdc_staging";
    public static final String COMMIT_TABLE = "commits";
    public static final String COORDINATION_TABLE = "coordination";
    public static final String PRIMARY_KEY_TABLE = "source_primary_keys";
    public static final String DROPPED_COLUMN_TABLE = "dropped_columns";
    public static final String GLOBAL_LOCK_ID = "_flink_cdc_global";
    public static final String TABLE_LOCK_ID = "_flink_cdc_table";

    public static boolean isInternalSchema(TableId tableId) {
        return INTERNAL_SCHEMA.equalsIgnoreCase(tableId.getSchemaName());
    }

    @Nullable
    public static String reservedSchemaName(TableId tableId) {
        if (isInternalSchema(tableId)) {
            return INTERNAL_SCHEMA;
        }
        if (STAGING_SCHEMA.equalsIgnoreCase(tableId.getSchemaName())) {
            return STAGING_SCHEMA;
        }
        return null;
    }

    public static String tableLockKey(TableId tableId) {
        if (tableId.getNamespace() != null) {
            throw new IllegalArgumentException("DuckLake table lock requires a two-part table ID");
        }
        String schema = tableId.getSchemaName() == null ? DEFAULT_SCHEMA : tableId.getSchemaName();
        return lengthPrefixed(normalizeIdentifier(schema))
                + lengthPrefixed(normalizeIdentifier(tableId.getTableName()));
    }

    private static String lengthPrefixed(String value) {
        return value.length() + ":" + value;
    }

    private static String normalizeIdentifier(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    private DuckLakeCatalogObjects() {}
}
