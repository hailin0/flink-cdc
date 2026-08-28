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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/** Path validation shared by built-in DuckLake providers. */
final class ProviderPathUtils {

    private ProviderPathUtils() {}

    static Path absoluteLocalPath(String option, String value) {
        final Path path;
        try {
            path = Paths.get(value).normalize();
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException(option + " must be a valid local path", e);
        }
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException(option + " must be an absolute path");
        }
        if (path.getParent() == null) {
            throw new IllegalArgumentException(option + " must not be the filesystem root");
        }
        return path;
    }

    static String remotePath(String option, String value, String... schemes) {
        final URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(option + " must be a valid URI", e);
        }
        String scheme = uri.getScheme();
        boolean supported =
                scheme != null
                        && Arrays.stream(schemes)
                                .anyMatch(candidate -> candidate.equalsIgnoreCase(scheme));
        if (!supported) {
            throw new IllegalArgumentException(option + " must use " + schemeList(schemes));
        }
        if (uri.getRawAuthority() == null
                || uri.getRawAuthority().isEmpty()
                || uri.getRawUserInfo() != null
                || uri.getPort() != -1
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null) {
            throw new IllegalArgumentException(
                    option
                            + " must contain a storage container and no credentials, port, query, or fragment");
        }
        return value;
    }

    static String withTrailingSlash(String path) {
        return path.endsWith("/") ? path : path + "/";
    }

    private static String schemeList(String[] schemes) {
        String[] values = Arrays.stream(schemes).map(value -> value + "://").toArray(String[]::new);
        if (values.length == 1) {
            return values[0];
        }
        if (values.length == 2) {
            return values[0] + " or " + values[1];
        }
        return String.join(", ", Arrays.copyOf(values, values.length - 1))
                + ", or "
                + values[values.length - 1];
    }
}
