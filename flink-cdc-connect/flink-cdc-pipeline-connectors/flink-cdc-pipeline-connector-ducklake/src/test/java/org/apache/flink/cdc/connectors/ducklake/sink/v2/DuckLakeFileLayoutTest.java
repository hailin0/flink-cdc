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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DuckLakeFileLayout}. */
class DuckLakeFileLayoutTest {

    @Test
    void buildsDataAndStagingPathsFromWriterIdentity() {
        DuckLakeFileLayout layout =
                new DuckLakeFileLayout("s3://warehouse/root/", 2, 3, "sink-id", "operator-id");
        TableId tableId = TableId.tableId("sales/..", "order name");

        assertThat(layout.dataFile(tableId, 5, 7, 11, "materialization"))
                .isEqualTo(
                        "s3://warehouse/root/sales%2F%2E%2E/order%20name/flink-cdc/"
                                + "73b0495504dbf5efa3f53b9273bbab6a65fceb1b4a262aa592e3cab03401e10e/"
                                + "5cc3ae36109e14ddb9b1417c9ebadad671c4e5855ce311b62d08a7bf1cbf3895/"
                                + "writer-epoch-5/batch-7/part-2-3-11-materialization.parquet");
        assertThat(layout.keyFile(tableId, 5, 7, 11, "materialization"))
                .isEqualTo(
                        "s3://warehouse/root/_flink_cdc_staging/keys/"
                                + "table-384e0df84e0f4556fd4e1c74cd022233930164b3c8492040797cdad3d08b32cd/"
                                + "73b0495504dbf5efa3f53b9273bbab6a65fceb1b4a262aa592e3cab03401e10e/"
                                + "5cc3ae36109e14ddb9b1417c9ebadad671c4e5855ce311b62d08a7bf1cbf3895/"
                                + "writer-epoch-5/batch-7/part-2-3-11-materialization.parquet");
    }
}
