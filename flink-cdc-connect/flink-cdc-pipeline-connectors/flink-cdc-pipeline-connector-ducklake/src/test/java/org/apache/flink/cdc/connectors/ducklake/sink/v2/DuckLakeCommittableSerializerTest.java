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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeCommittableSerializerTest {

    private final DuckLakeCommittableSerializer serializer = new DuckLakeCommittableSerializer();

    @Test
    void roundTripsEveryFieldAndSupportedType() throws Exception {
        DuckLakeCommittable committable = DuckLakeTestData.committable(null, 0);

        byte[] serialized = serializer.serialize(committable);

        assertThat(serializer.deserialize(serializer.getVersion(), serialized))
                .isEqualTo(committable);
    }

    @Test
    void rejectsUnknownVersion() throws Exception {
        byte[] serialized = serializer.serialize(DuckLakeTestData.committable(null, 0));

        assertThatThrownBy(() -> serializer.deserialize(999, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version: 999");
    }
}
