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

package org.apache.flink.cdc.connectors.ducklake.sink;

import org.apache.flink.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

/** SQL statements and local state produced while planning one schema change. */
final class DuckLakeSchemaChangePlan {

    private final List<String> statements;
    @Nullable private final Schema schemaAfterCommit;
    @Nullable private final Set<String> droppedColumnsAfterCommit;

    DuckLakeSchemaChangePlan(
            List<String> statements,
            @Nullable Schema schemaAfterCommit,
            @Nullable Set<String> droppedColumnsAfterCommit) {
        this.statements = statements;
        this.schemaAfterCommit = schemaAfterCommit;
        this.droppedColumnsAfterCommit = droppedColumnsAfterCommit;
    }

    List<String> statements() {
        return statements;
    }

    @Nullable
    Schema schemaAfterCommit() {
        return schemaAfterCommit;
    }

    @Nullable
    Set<String> droppedColumnsAfterCommit() {
        return droppedColumnsAfterCommit;
    }
}
