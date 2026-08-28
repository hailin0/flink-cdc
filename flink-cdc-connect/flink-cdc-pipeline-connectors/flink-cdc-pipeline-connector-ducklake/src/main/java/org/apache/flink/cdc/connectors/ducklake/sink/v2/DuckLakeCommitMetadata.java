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

/** DuckLake snapshot metadata written for each completed Flink checkpoint. */
final class DuckLakeCommitMetadata {

    static final String AUTHOR = "flink-cdc";
    static final String MESSAGE_PREFIX = "Flink CDC checkpoint ";

    private static final String SEMANTIC = "primary-key-current-state";
    private static final int PROTOCOL_VERSION = 1;

    static String extraInfo(String sinkId, String operatorId, long checkpointId, String planHash) {
        return "{\"connector\":"
                + jsonString(AUTHOR)
                + ",\"semantic\":"
                + jsonString(SEMANTIC)
                + ",\"protocol_version\":"
                + PROTOCOL_VERSION
                + ",\"sink_instance_id\":"
                + jsonString(sinkId)
                + ",\"operator_id\":"
                + jsonString(operatorId)
                + ",\"checkpoint_id\":"
                + checkpointId
                + ",\"plan_hash\":"
                + jsonString(planHash)
                + "}";
    }

    private static String jsonString(String value) {
        StringBuilder result = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            switch (current) {
                case '"':
                    result.append("\\\"");
                    break;
                case '\\':
                    result.append("\\\\");
                    break;
                case '\b':
                    result.append("\\b");
                    break;
                case '\f':
                    result.append("\\f");
                    break;
                case '\n':
                    result.append("\\n");
                    break;
                case '\r':
                    result.append("\\r");
                    break;
                case '\t':
                    result.append("\\t");
                    break;
                default:
                    if (current < 0x20) {
                        result.append(String.format("\\u%04x", (int) current));
                    } else {
                        result.append(current);
                    }
            }
        }
        return result.append('"').toString();
    }

    private DuckLakeCommitMetadata() {}
}
