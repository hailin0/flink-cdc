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
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Maps Flink CDC types to types with a lossless DuckDB and DuckLake round trip. */
@Internal
public final class DuckLakeTypeUtils {

    private static final int DUCKDB_MAX_DECIMAL_PRECISION = 38;
    private static final int DUCKLAKE_TEMPORAL_MICROSECOND_PRECISION = 6;
    private static final Pattern DECIMAL_TYPE = Pattern.compile("DECIMAL\\((\\d+),(\\d+)\\)");

    public static String toDuckDbType(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return "VARCHAR";
            case BOOLEAN:
                return "BOOLEAN";
            case BINARY:
            case VARBINARY:
                return "BLOB";
            case DECIMAL:
                int decimalPrecision = DataTypes.getPrecision(type).orElse(0);
                int decimalScale = DataTypes.getScale(type).orElse(0);
                if (decimalPrecision > DUCKDB_MAX_DECIMAL_PRECISION) {
                    throw unsupported(type, "decimal precision exceeds 38");
                }
                return String.format("DECIMAL(%d,%d)", decimalPrecision, decimalScale);
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                int timePrecision = DataTypes.getPrecision(type).orElse(0);
                if (timePrecision > DUCKLAKE_TEMPORAL_MICROSECOND_PRECISION) {
                    throw unsupported(type, "DuckLake TIME has microsecond precision");
                }
                return "TIME";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampType(DataTypes.getPrecision(type).orElse(0));
            case TIMESTAMP_WITH_TIME_ZONE:
                throw unsupported(type, "zoned timestamp offsets cannot round trip");
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int localTimestampPrecision = DataTypes.getPrecision(type).orElse(0);
                if (localTimestampPrecision > DUCKLAKE_TEMPORAL_MICROSECOND_PRECISION) {
                    throw unsupported(type, "DuckDB TIMESTAMPTZ has microsecond precision");
                }
                return "TIMESTAMPTZ";
            case ARRAY:
            case MAP:
            case ROW:
            case VARIANT:
                throw unsupported(type, "type is not supported in the first release");
            default:
                throw unsupported(type, "unknown type root");
        }
    }

    public static boolean isLosslessPromotion(DataType from, DataType to) {
        if (from.equals(to) || from.copy(true).equals(to.copy(true))) {
            return true;
        }
        if (isSignedInteger(from.getTypeRoot()) && isSignedInteger(to.getTypeRoot())) {
            return signedIntegerRank(from.getTypeRoot()) < signedIntegerRank(to.getTypeRoot());
        }
        if (from.is(DataTypeRoot.FLOAT) && to.is(DataTypeRoot.DOUBLE)) {
            return true;
        }
        if (from.is(DataTypeRoot.DECIMAL) && to.is(DataTypeRoot.DECIMAL)) {
            int fromPrecision =
                    DataTypes.getPrecision(from).orElseThrow(IllegalStateException::new);
            int fromScale = DataTypes.getScale(from).orElseThrow(IllegalStateException::new);
            int toPrecision = DataTypes.getPrecision(to).orElseThrow(IllegalStateException::new);
            int toScale = DataTypes.getScale(to).orElseThrow(IllegalStateException::new);
            return toScale >= fromScale
                    && toPrecision - toScale >= fromPrecision - fromScale
                    && toPrecision <= DUCKDB_MAX_DECIMAL_PRECISION;
        }
        if (isStringPromotion(from, to) || isBinaryPromotion(from, to)) {
            return DataTypes.getLength(to).orElse(Integer.MAX_VALUE)
                    >= DataTypes.getLength(from).orElse(Integer.MAX_VALUE);
        }
        if (from.getTypeRoot() == to.getTypeRoot()
                && from.isAnyOf(
                        DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                        DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                        DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            try {
                toDuckDbType(to);
                return DataTypes.getPrecision(to).orElse(0)
                        >= DataTypes.getPrecision(from).orElse(0);
            } catch (IllegalArgumentException ignored) {
                return false;
            }
        }
        return false;
    }

    /** Returns whether DuckLake supports the promotion through a direct ALTER COLUMN operation. */
    public static boolean isSupportedDdlPromotion(DataType from, DataType to) {
        if (normalizeDuckDbType(toDuckDbType(from)).equals(normalizeDuckDbType(toDuckDbType(to)))) {
            return true;
        }
        DataTypeRoot source = from.getTypeRoot();
        DataTypeRoot target = to.getTypeRoot();
        if (source == DataTypeRoot.TINYINT) {
            return target == DataTypeRoot.SMALLINT
                    || target == DataTypeRoot.INTEGER
                    || target == DataTypeRoot.BIGINT;
        }
        if (source == DataTypeRoot.SMALLINT) {
            return target == DataTypeRoot.INTEGER || target == DataTypeRoot.BIGINT;
        }
        return (source == DataTypeRoot.INTEGER && target == DataTypeRoot.BIGINT)
                || (source == DataTypeRoot.FLOAT && target == DataTypeRoot.DOUBLE);
    }

    /** Returns whether a staged file type can be imported into an existing target column. */
    public static boolean isCompatibleTargetType(DataType sourceType, String targetType) {
        String source = normalizeDuckDbType(toDuckDbType(sourceType));
        String target = normalizeDuckDbType(targetType);
        if (source.equals(target)) {
            return true;
        }
        if (source.equals("TINYINT")) {
            return target.equals("SMALLINT") || target.equals("INTEGER") || target.equals("BIGINT");
        }
        if (source.equals("SMALLINT")) {
            return target.equals("INTEGER") || target.equals("BIGINT");
        }
        return (source.equals("INTEGER") && target.equals("BIGINT"))
                || (source.equals("FLOAT") && target.equals("DOUBLE"));
    }

    public static String normalizeDuckDbType(String type) {
        String normalized = type.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        if (normalized.equals("TIMESTAMP WITH TIME ZONE")) {
            return "TIMESTAMPTZ";
        }
        if (normalized.equals("TIMESTAMP WITHOUT TIME ZONE")) {
            return "TIMESTAMP";
        }
        if (normalized.equals("DOUBLE PRECISION")) {
            return "DOUBLE";
        }
        if (normalized.equals("INT")) {
            return "INTEGER";
        }
        return normalized.replace(", ", ",");
    }

    public static DataType fromDuckDbType(String type, boolean nullable) {
        String normalized = normalizeDuckDbType(type);
        DataType result;
        switch (normalized) {
            case "VARCHAR":
                result = DataTypes.STRING();
                break;
            case "BOOLEAN":
                result = DataTypes.BOOLEAN();
                break;
            case "BLOB":
                result = DataTypes.BYTES();
                break;
            case "TINYINT":
                result = DataTypes.TINYINT();
                break;
            case "SMALLINT":
                result = DataTypes.SMALLINT();
                break;
            case "INTEGER":
                result = DataTypes.INT();
                break;
            case "BIGINT":
                result = DataTypes.BIGINT();
                break;
            case "FLOAT":
                result = DataTypes.FLOAT();
                break;
            case "DOUBLE":
                result = DataTypes.DOUBLE();
                break;
            case "DATE":
                result = DataTypes.DATE();
                break;
            case "TIME":
                result = DataTypes.TIME(DUCKLAKE_TEMPORAL_MICROSECOND_PRECISION);
                break;
            case "TIMESTAMP_S":
                result = DataTypes.TIMESTAMP(0);
                break;
            case "TIMESTAMP_MS":
                result = DataTypes.TIMESTAMP(3);
                break;
            case "TIMESTAMP":
            case "TIMESTAMP_US":
                result = DataTypes.TIMESTAMP(6);
                break;
            case "TIMESTAMP_NS":
                result = DataTypes.TIMESTAMP(9);
                break;
            case "TIMESTAMPTZ":
                result = DataTypes.TIMESTAMP_LTZ(DUCKLAKE_TEMPORAL_MICROSECOND_PRECISION);
                break;
            default:
                Matcher decimal = DECIMAL_TYPE.matcher(normalized);
                if (!decimal.matches()) {
                    throw new IllegalArgumentException("Unsupported DuckDB catalog type: " + type);
                }
                result =
                        DataTypes.DECIMAL(
                                Integer.parseInt(decimal.group(1)),
                                Integer.parseInt(decimal.group(2)));
        }
        return nullable ? result.nullable() : result.notNull();
    }

    public static boolean isCompatibleParquetType(DataType declaredType, String parquetType) {
        String declared = normalizeDuckDbType(toDuckDbType(declaredType));
        String actual = normalizeDuckDbType(parquetType);
        return declared.equals(actual)
                || ((declared.equals("TIMESTAMP_S") || declared.equals("TIMESTAMP_MS"))
                        && actual.equals("TIMESTAMP"));
    }

    public static FieldBinder createFieldBinder(
            DataType type, int fieldPosition, ZoneId pipelineZone) {
        toDuckDbType(type);
        RecordData.FieldGetter fieldGetter = RecordData.createFieldGetter(type, fieldPosition);
        int jdbcType = toJdbcType(type);
        return (statement, parameterIndex, row) -> {
            Object value = fieldGetter.getFieldOrNull(row);
            if (value == null) {
                statement.setNull(parameterIndex, jdbcType);
                return;
            }
            bindNonNull(statement, parameterIndex, type.getTypeRoot(), value, pipelineZone);
        };
    }

    private static void bindNonNull(
            PreparedStatement statement,
            int parameterIndex,
            DataTypeRoot typeRoot,
            Object value,
            ZoneId pipelineZone)
            throws SQLException {
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                statement.setString(parameterIndex, ((StringData) value).toString());
                break;
            case BOOLEAN:
                statement.setBoolean(parameterIndex, (boolean) value);
                break;
            case BINARY:
            case VARBINARY:
                statement.setBytes(parameterIndex, (byte[]) value);
                break;
            case DECIMAL:
                statement.setBigDecimal(parameterIndex, ((DecimalData) value).toBigDecimal());
                break;
            case TINYINT:
                statement.setByte(parameterIndex, (byte) value);
                break;
            case SMALLINT:
                statement.setShort(parameterIndex, (short) value);
                break;
            case INTEGER:
                statement.setInt(parameterIndex, (int) value);
                break;
            case BIGINT:
                statement.setLong(parameterIndex, (long) value);
                break;
            case FLOAT:
                statement.setFloat(parameterIndex, (float) value);
                break;
            case DOUBLE:
                statement.setDouble(parameterIndex, (double) value);
                break;
            case DATE:
                statement.setObject(parameterIndex, ((DateData) value).toLocalDate());
                break;
            case TIME_WITHOUT_TIME_ZONE:
                statement.setObject(parameterIndex, ((TimeData) value).toLocalTime());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                statement.setObject(parameterIndex, ((TimestampData) value).toLocalDateTime());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                statement.setObject(
                        parameterIndex,
                        ((LocalZonedTimestampData) value)
                                .toInstant()
                                .atZone(pipelineZone)
                                .toOffsetDateTime());
                break;
            default:
                throw new IllegalArgumentException("Unsupported JDBC binding type: " + typeRoot);
        }
    }

    private static int toJdbcType(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return Types.VARCHAR;
            case BOOLEAN:
                return Types.BOOLEAN;
            case BINARY:
            case VARBINARY:
                return Types.BLOB;
            case DECIMAL:
                return Types.DECIMAL;
            case TINYINT:
                return Types.TINYINT;
            case SMALLINT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case BIGINT:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DATE:
                return Types.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                return Types.TIME;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Types.TIMESTAMP;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Types.TIMESTAMP_WITH_TIMEZONE;
            default:
                throw unsupported(type, "no JDBC binding");
        }
    }

    private static String timestampType(int precision) {
        if (precision == 0) {
            return "TIMESTAMP_S";
        }
        if (precision <= 3) {
            return "TIMESTAMP_MS";
        }
        if (precision <= 6) {
            return "TIMESTAMP";
        }
        return "TIMESTAMP_NS";
    }

    private static boolean isSignedInteger(DataTypeRoot root) {
        return root == DataTypeRoot.TINYINT
                || root == DataTypeRoot.SMALLINT
                || root == DataTypeRoot.INTEGER
                || root == DataTypeRoot.BIGINT;
    }

    private static int signedIntegerRank(DataTypeRoot root) {
        switch (root) {
            case TINYINT:
                return 0;
            case SMALLINT:
                return 1;
            case INTEGER:
                return 2;
            case BIGINT:
                return 3;
            default:
                throw new IllegalArgumentException("Not a signed integer type: " + root);
        }
    }

    private static boolean isStringPromotion(DataType from, DataType to) {
        return from.isAnyOf(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR)
                && to.isAnyOf(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR);
    }

    private static boolean isBinaryPromotion(DataType from, DataType to) {
        return from.isAnyOf(DataTypeRoot.BINARY, DataTypeRoot.VARBINARY)
                && to.isAnyOf(DataTypeRoot.BINARY, DataTypeRoot.VARBINARY);
    }

    private static IllegalArgumentException unsupported(DataType type, String reason) {
        return new IllegalArgumentException(
                "Unsupported DuckLake type "
                        + type.getTypeRoot()
                        + " ("
                        + type.asSummaryString()
                        + "): "
                        + reason);
    }

    /** Binds one field from a CDC record to a DuckDB prepared statement parameter. */
    @FunctionalInterface
    public interface FieldBinder {
        void bind(PreparedStatement statement, int parameterIndex, RecordData row)
                throws SQLException;
    }

    private DuckLakeTypeUtils() {}
}
