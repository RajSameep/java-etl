package io.github.veerakumarak.etl.parquet.converters;

import io.github.veerakumarak.etl.parquet.data.DataAnnotationHelper;
import io.github.veerakumarak.etl.utils.DateUtil;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GroupToClassConverter {

    private static final Logger log = LoggerFactory.getLogger(GroupToClassConverter.class);

    public static Object[] convert(Group group, Field[] classFields, Map<String, Type> fieldsMap, boolean relaxedValidation) {
        return Arrays.stream(classFields).map(field -> {
            String fieldName = DataAnnotationHelper.getName(field);
            if (!fieldsMap.containsKey(fieldName)) {
                if (relaxedValidation) {
                    // Field not in Parquet schema - return null (allows reading older files without new columns)
                    log.warn("Field '{}' not found in Parquet schema, returning null", fieldName);
                    return null;
                } else {
                    throw new InternalError("parameterName missing " + fieldName);
                }
            }
            return fromGroup(group, fieldName, fieldsMap.get(fieldName).asPrimitiveType(), field.getType());
        }).toArray();
    }


    private static Object fromGroup(Group group, String fieldName, PrimitiveType primitiveType, Class<?> targetType) {

        int fieldIndex = group.getType().getFieldIndex(fieldName);
        if (fieldIndex < 0 || group.getFieldRepetitionCount(fieldIndex) == 0) {
            return null; // Field is not present
        }

        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();

        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
            int scale = decimalType.getScale();
            return switch (primitiveTypeName) {
                case INT32 -> BigDecimal.valueOf(group.getInteger(fieldName, 0), scale).floatValue();
                case INT64 -> BigDecimal.valueOf(group.getLong(fieldName, 0), scale).floatValue();
                case FIXED_LEN_BYTE_ARRAY, BINARY -> {
                    Binary unscaledBinary = group.getBinary(fieldName, 0);
//                    if (unscaledBinary.length() == 0) {
//                        yield null;
//                    }
                    BigInteger unscaledBigInt = new BigInteger(unscaledBinary.getBytesUnsafe());
                    yield new BigDecimal(unscaledBigInt, scale).floatValue();
                }
                default ->
                        throw new IllegalArgumentException("Unsupported primitive type for DECIMAL: " + primitiveTypeName);
            };
        } else if (targetType == LocalDate.class) {
            // Handle DATE logical type (INT32 epoch days)
            if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                return switch (primitiveTypeName) {
                    case INT32 -> LocalDate.ofEpochDay(group.getInteger(fieldName, 0));
                    default ->
                            throw new IllegalArgumentException("DATE logical type should be stored as INT32. Found: " + primitiveTypeName);
                };
            }
            // Handle TIMESTAMP -> LocalDate conversion (extract date part from timestamp)
            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType &&
                    primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                long epochMillis = group.getLong(fieldName, 0);
                return Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of("UTC")).toLocalDate();
            }
            // Handle raw INT64 timestamp without logical type annotation -> LocalDate
            if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64 && logicalType == null) {
                long epochMillis = group.getLong(fieldName, 0);
                return Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of("UTC")).toLocalDate();
            }
            throw new IllegalArgumentException("Unsupported type for LocalDate: " + primitiveType + " with logical type: " + logicalType);
        } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeType && targetType == LocalTime.class) {
            if (timeType.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
                return switch (primitiveTypeName) {
                    case INT32 -> LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(group.getInteger(fieldName, 0)));
                    default ->
                            throw new IllegalArgumentException("TIME(MILLIS) logical type should be stored as INT32. Found: " + primitiveTypeName);
                };
            } else {
                throw new IllegalArgumentException("Unsupported TIME unit: " + timeType.getUnit() + ". Only MILLIS is supported for LocalTime with INT32.");
            }
        } else if (targetType == LocalDateTime.class) {
            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType &&
                    tsType.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS &&
                    primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                long epochMicros = group.getLong(fieldName, 0);
                return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochMicros / 1_000_000, (epochMicros % 1_000_000) * 1000), ZoneId.of("UTC"));
            } else if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType2 &&
                    tsType2.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS &&
                    primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                return DateUtil.convertEpochMiliSecToLocalDateTime(group.getLong(fieldName, 0), ZoneId.of("UTC"));
            } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64 && logicalType == null) {
                return DateUtil.convertEpochMiliSecToLocalDateTime(group.getLong(fieldName, 0), ZoneId.of("UTC"));
            } else {
                throw new IllegalArgumentException("Unsupported type for LocalDateTime: " + primitiveType + " with logical type: " + logicalType);
            }
        } else if (targetType == Character.class) {
            return switch (primitiveTypeName) {
                case BINARY -> {
                    String value = group.getBinary(fieldName, 0).toStringUsingUTF8();
                    if (value.length() > 1) {
                        throw new IllegalArgumentException("Cannot convert BINARY string of length " + value.length() + " to Character. Expected length 1 or empty.");
                    }
                    yield value.length() == 1 ? value.charAt(0) : null; // Handle empty string as null
                }
                default ->
                        throw new IllegalArgumentException("Unsupported primitive type for Character: " + primitiveTypeName + ". Expected BINARY.");
            };
        } else {
//            if (logicalType == LogicalTypeAnnotation.stringType() && primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY && targetType == String.class) {
//                return group.getBinary(fieldName, 0).toStringUsingUTF8();
//            }
            return switch (primitiveTypeName) {
                case INT32 -> group.getInteger(fieldName, 0);
                case INT64 -> group.getLong(fieldName, 0);
                case FLOAT -> group.getFloat(fieldName, 0);
                case DOUBLE -> group.getDouble(fieldName, 0);
                case BOOLEAN -> group.getBoolean(fieldName, 0);
                case BINARY -> group.getBinary(fieldName, 0).toStringUsingUTF8();
                default -> {
                    try {
                        yield group.getString(fieldName, 0);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Unsupported primitive type: " + primitiveTypeName +
                                " and failed to convert to String.", e);
                    }
                }
            };
        }
    }

}
