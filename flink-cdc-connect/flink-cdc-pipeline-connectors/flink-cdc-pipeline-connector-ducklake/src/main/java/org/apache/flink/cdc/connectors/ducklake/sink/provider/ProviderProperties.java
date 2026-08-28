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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Validates and reads provider-specific properties. */
final class ProviderProperties {

    private final String prefix;
    private final Map<String, String> properties;

    ProviderProperties(
            String prefix,
            Map<String, String> properties,
            Set<String> required,
            Set<String> optional) {
        this.prefix = prefix;
        this.properties = properties;
        Set<String> supported = new HashSet<>(required);
        supported.addAll(optional);
        for (String key : properties.keySet()) {
            if (!supported.contains(key)) {
                throw new IllegalArgumentException("Unsupported option '" + prefix + key + "'");
            }
        }
        for (String key : required) {
            required(key);
        }
    }

    String required(String key) {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required option '" + prefix + key + "'");
        }
        return value;
    }

    String optional(String key, String defaultValue) {
        String value = properties.get(key);
        return value == null ? defaultValue : value;
    }

    String optional(String key) {
        return properties.get(key);
    }

    int integer(String key, int defaultValue) {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Option '" + prefix + key + "' must be an integer", e);
        }
    }

    boolean bool(String key, boolean defaultValue) {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException(
                    "Option '" + prefix + key + "' must be true or false");
        }
        return Boolean.parseBoolean(value);
    }
}
