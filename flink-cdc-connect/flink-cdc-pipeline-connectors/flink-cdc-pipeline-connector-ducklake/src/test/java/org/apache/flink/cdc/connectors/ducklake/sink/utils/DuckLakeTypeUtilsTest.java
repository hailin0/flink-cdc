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

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DuckLakeTypeUtils}. */
class DuckLakeTypeUtilsTest {

    @TempDir Path tempDir;

    @Test
    void mapsSupportedPrimitiveTypes() {
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.BOOLEAN())).isEqualTo("BOOLEAN");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.TINYINT())).isEqualTo("TINYINT");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.SMALLINT())).isEqualTo("SMALLINT");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.INT())).isEqualTo("INTEGER");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.BIGINT())).isEqualTo("BIGINT");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.FLOAT())).isEqualTo("FLOAT");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.DOUBLE())).isEqualTo("DOUBLE");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.DECIMAL(20, 4)))
                .isEqualTo("DECIMAL(20,4)");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.CHAR(10))).isEqualTo("VARCHAR");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.STRING())).isEqualTo("VARCHAR");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.BINARY(10))).isEqualTo("BLOB");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.BYTES())).isEqualTo("BLOB");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.DATE())).isEqualTo("DATE");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.TIME(6))).isEqualTo("TIME");
    }

    @Test
    void preservesTimestampPrecision() {
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.TIMESTAMP(0))).isEqualTo("TIMESTAMP_S");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.TIMESTAMP(3)))
                .isEqualTo("TIMESTAMP_MS");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.TIMESTAMP(6))).isEqualTo("TIMESTAMP");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.TIMESTAMP(9)))
                .isEqualTo("TIMESTAMP_NS");
        assertThat(DuckLakeTypeUtils.toDuckDbType(DataTypes.TIMESTAMP_LTZ(6)))
                .isEqualTo("TIMESTAMPTZ");
    }

    @Test
    void reconstructsCdcTypesFromDuckDbCatalogTypes() {
        assertThat(DuckLakeTypeUtils.fromDuckDbType("BIGINT", false))
                .isEqualTo(DataTypes.BIGINT().notNull());
        assertThat(DuckLakeTypeUtils.fromDuckDbType("DECIMAL(20, 4)", true))
                .isEqualTo(DataTypes.DECIMAL(20, 4));
        assertThat(DuckLakeTypeUtils.fromDuckDbType("TIMESTAMP_MS", true))
                .isEqualTo(DataTypes.TIMESTAMP(3));
        assertThat(DuckLakeTypeUtils.fromDuckDbType("TIMESTAMP WITH TIME ZONE", true))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ(6));
        assertThat(DuckLakeTypeUtils.fromDuckDbType("BLOB", true)).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void matchesTypesReportedForWriterParquetFiles() throws Exception {
        List<DataType> types =
                Arrays.asList(
                        DataTypes.BOOLEAN(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(20, 4),
                        DataTypes.STRING(),
                        DataTypes.BYTES(),
                        DataTypes.DATE(),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(0),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP(9),
                        DataTypes.TIMESTAMP_LTZ(6));
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            for (int i = 0; i < types.size(); i++) {
                DataType type = types.get(i);
                String duckDbType = DuckLakeTypeUtils.toDuckDbType(type);
                String parquet = tempDir.resolve("type-" + i + ".parquet").toString();
                statement.execute("CREATE TEMP TABLE writer_buffer (value " + duckDbType + ")");
                statement.execute(
                        "COPY writer_buffer TO '"
                                + parquet.replace("'", "''")
                                + "' (FORMAT PARQUET)");
                try (ResultSet resultSet =
                        statement.executeQuery(
                                "DESCRIBE SELECT * FROM read_parquet('"
                                        + parquet.replace("'", "''")
                                        + "')")) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(
                                    DuckLakeTypeUtils.isCompatibleParquetType(
                                            type, resultSet.getString(2)))
                            .as(type.asSummaryString())
                            .isTrue();
                }
                statement.execute("DROP TABLE writer_buffer");
            }
        }
    }

    @Test
    void rejectsTypesWithoutLosslessRoundTrip() {
        assertUnsupported(DataTypes.ARRAY(DataTypes.INT()), "ARRAY");
        assertUnsupported(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), "MAP");
        assertUnsupported(DataTypes.ROW(DataTypes.INT()), "ROW");
        assertUnsupported(DataTypes.VARIANT(), "VARIANT");
        assertUnsupported(DataTypes.TIMESTAMP_TZ(6), "TIMESTAMP_WITH_TIME_ZONE");
        assertUnsupported(DataTypes.TIME(9), "TIME(9)");
        assertUnsupported(DataTypes.TIMESTAMP_LTZ(9), "TIMESTAMP_LTZ(9)");
    }

    @Test
    void acceptsOnlyLosslessTypePromotions() {
        assertThat(DuckLakeTypeUtils.isLosslessPromotion(DataTypes.INT(), DataTypes.BIGINT()))
                .isTrue();
        assertThat(DuckLakeTypeUtils.isLosslessPromotion(DataTypes.FLOAT(), DataTypes.DOUBLE()))
                .isTrue();
        assertThat(
                        DuckLakeTypeUtils.isLosslessPromotion(
                                DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(12, 2)))
                .isTrue();
        assertThat(DuckLakeTypeUtils.isLosslessPromotion(DataTypes.BIGINT(), DataTypes.INT()))
                .isFalse();
        assertThat(
                        DuckLakeTypeUtils.isLosslessPromotion(
                                DataTypes.DECIMAL(12, 2), DataTypes.DECIMAL(10, 2)))
                .isFalse();
        assertThat(
                        DuckLakeTypeUtils.isLosslessPromotion(
                                DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(12, 4)))
                .isTrue();
    }

    @Test
    void recognizesDuckLakeDdlPromotions() {
        assertThat(
                        DuckLakeTypeUtils.isSupportedDdlPromotion(
                                DataTypes.TINYINT(), DataTypes.BIGINT()))
                .isTrue();
        assertThat(DuckLakeTypeUtils.isSupportedDdlPromotion(DataTypes.FLOAT(), DataTypes.DOUBLE()))
                .isTrue();
        assertThat(DuckLakeTypeUtils.isSupportedDdlPromotion(DataTypes.INT(), DataTypes.SMALLINT()))
                .isFalse();
        assertThat(
                        DuckLakeTypeUtils.isSupportedDdlPromotion(
                                DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(12, 2)))
                .isFalse();
    }

    @Test
    void recognizesCompatibleDuckLakeTargetTypes() {
        assertThat(DuckLakeTypeUtils.isCompatibleTargetType(DataTypes.TINYINT(), "BIGINT"))
                .isTrue();
        assertThat(DuckLakeTypeUtils.isCompatibleTargetType(DataTypes.FLOAT(), "DOUBLE PRECISION"))
                .isTrue();
        assertThat(DuckLakeTypeUtils.isCompatibleTargetType(DataTypes.BIGINT(), "INTEGER"))
                .isFalse();
    }

    @Test
    void bindsLocalZonedTimestampUsingPipelineZone() throws Exception {
        List<Object[]> calls = new ArrayList<>();
        PreparedStatement statement = recordingPreparedStatement(calls);
        GenericRecordData row = new GenericRecordData(1);
        row.setField(
                0,
                LocalZonedTimestampData.fromInstant(Instant.parse("2024-01-01T00:00:00.123456Z")));

        DuckLakeTypeUtils.createFieldBinder(
                        DataTypes.TIMESTAMP_LTZ(6), 0, ZoneId.of("Asia/Shanghai"))
                .bind(statement, 2, row);

        assertThat(calls).hasSize(1);
        assertThat(calls.get(0)[0]).isEqualTo("setObject");
        assertThat(calls.get(0)[1]).isEqualTo(2);
        assertThat(calls.get(0)[2])
                .isEqualTo(OffsetDateTime.parse("2024-01-01T08:00:00.123456+08:00"));
    }

    @Test
    void bindsNullWithTheMatchingJdbcType() throws Exception {
        List<Object[]> calls = new ArrayList<>();
        GenericRecordData row = new GenericRecordData(1);

        DuckLakeTypeUtils.createFieldBinder(DataTypes.TIMESTAMP_LTZ(6), 0, ZoneId.of("UTC"))
                .bind(recordingPreparedStatement(calls), 1, row);

        assertThat(calls).hasSize(1);
        assertThat(calls.get(0)).containsExactly("setNull", 1, Types.TIMESTAMP_WITH_TIMEZONE);
    }

    private static void assertUnsupported(Object type, String expectedMessage) {
        assertThatThrownBy(
                        () ->
                                DuckLakeTypeUtils.toDuckDbType(
                                        (org.apache.flink.cdc.common.types.DataType) type))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedMessage);
    }

    private static PreparedStatement recordingPreparedStatement(List<Object[]> calls) {
        return (PreparedStatement)
                Proxy.newProxyInstance(
                        DuckLakeTypeUtilsTest.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        (proxy, method, args) -> {
                            if (method.getName().startsWith("set")) {
                                Object[] call = new Object[args.length + 1];
                                call[0] = method.getName();
                                System.arraycopy(args, 0, call, 1, args.length);
                                calls.add(call);
                            }
                            return null;
                        });
    }
}
