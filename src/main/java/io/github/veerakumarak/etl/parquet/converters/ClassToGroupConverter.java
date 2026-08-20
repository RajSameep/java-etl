package io.github.veerakumarak.etl.parquet.converters;

import io.github.veerakumarak.etl.parquet.ClassHelper;
import io.github.veerakumarak.etl.parquet.data.DataAnnotationHelper;
import io.github.veerakumarak.etl.utils.DateUtil;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InternalFailure;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClassToGroupConverter {

    private static final Logger log = LoggerFactory.getLogger(ClassToGroupConverter.class);

    public static <T> List<Group> toGroup(List<T> tList, MessageType schema) {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        return tList.stream()
                .map(record -> toGroup(record, groupFactory))
                .toList();
    }

    public static <T> Group toGroup(T t, SimpleGroupFactory groupFactory) {
        Group group = groupFactory.newGroup();
        Class<?> tClass = t.getClass();

        for (Field field : tClass.getDeclaredFields()) {
            String name = DataAnnotationHelper.getName(field);

            if (field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
                continue;
            }

            try {
                field.setAccessible(true);
                Object value = field.get(t);
                if (Objects.isNull(value)) {
                    continue;
                }
                if (value instanceof Integer i) {
                    group.append(name, i);
                } else if (value instanceof Long l) {
                    group.append(name, l);
                } else if (value instanceof Double v) {
                    group.append(name, v);
                } else if (value instanceof Float v) {
                    group.append(name, v);
                } else if (value instanceof Boolean b) {
                    group.append(name, b);
                } else if (value instanceof Character c) {
                    group.append(name, String.valueOf(c));
                } else if (value instanceof String s) {
                    group.append(name, s);
                } else if (value instanceof LocalTime lt) {
                    group.append(name, (int) TimeUnit.NANOSECONDS.toMillis(lt.toNanoOfDay()));
                } else if (value instanceof LocalDate ld) {
                    group.append(name, (int) ld.toEpochDay());
                } else if (value instanceof LocalDateTime ldt) {
                    long epochMicros = ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + ldt.getNano() / 1000;
                    group.append(name, epochMicros);
                } else {
                    // Default / No-op
                }
                // Java 21
//                switch (value) {
//                    case Integer i -> group.append(name, i);
//                    case Long l -> group.append(name, l);
//                    case Double v -> group.append(name, v);
//                    case Float v -> group.append(name, v);
//                    case Boolean b -> group.append(name, b);
//                    case Character c -> group.append(name, String.valueOf(c));
//                    case String s -> group.append(name, s);
//                    case LocalTime lt -> group.append(name, (int) TimeUnit.NANOSECONDS.toMillis(lt.toNanoOfDay()));
//                    case LocalDate ld -> group.append(name, (int) ld.toEpochDay());
//                    case LocalDateTime ldt -> group.append(name, ldt.toInstant(ZoneOffset.UTC).toEpochMilli());
//                    default -> {
//                    }
//                }
            } catch (IllegalAccessException e) {
                throw new InternalFailure("creating fields from group value caused issue");
            }
        }
        return group;
    }

//    public static <T> List<T> fromGroup(List<Group> groups, Class<T> tClass, List<Type> parquetFields, Field[] classFields, boolean relaxedValidation) {
//        Constructor<?> constructor = ClassHelper.getAllArgsConstructor(tClass)
//                .orElseThrow(() -> new InternalFailure("Could not find AllArgsConstructor for Class: " + tClass.getName()));
//
//        Map<String, Type> fieldsMap = parquetFields.stream().collect(Collectors.toMap(Type::getName, field -> field));
//
//        return groups.stream()
//                .map(group -> fromGroup(group, classFields, fieldsMap, relaxedValidation))
//                .map(fieldValues -> Result.of(() -> (T) constructor.newInstance(fieldValues)))
//                .filter(Result::isOk)
//                .map(Result::get)
//                .toList();
//    }

//    private static Object[] fromGroup(Group group, Field[] classFields, Map<String, Type> fieldsMap, boolean relaxedValidation) {
//        return Arrays.stream(classFields).map(field -> {
//            String fieldName = DataAnnotationHelper.getName(field);
//            if (!fieldsMap.containsKey(fieldName)) {
//                if (relaxedValidation) {
//                    // Field not in Parquet schema - return null (allows reading older files without new columns)
//                    log.warn("Field '{}' not found in Parquet schema, returning null", fieldName);
//                    return null;
//                } else {
//                    throw new InternalFailure("parameterName missing " + fieldName);
//                }
//            }
//            return fromGroup(group, fieldName, fieldsMap.get(fieldName).asPrimitiveType(), field.getType());
//        }).toArray();
//    }


//    private static Object fromGroup(Group group, String fieldName, PrimitiveType primitiveType, Class<?> targetType) {
//
//        int fieldIndex = group.getType().getFieldIndex(fieldName);
//        if (fieldIndex < 0 || group.getFieldRepetitionCount(fieldIndex) == 0) {
//            return null; // Field is not present
//        }
//
//        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
//        PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();
//
//        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
//            int scale = decimalType.getScale();
//            return switch (primitiveTypeName) {
//                case INT32 -> BigDecimal.valueOf(group.getInteger(fieldName, 0), scale).floatValue();
//                case INT64 -> BigDecimal.valueOf(group.getLong(fieldName, 0), scale).floatValue();
//                case FIXED_LEN_BYTE_ARRAY, BINARY -> {
//                    Binary unscaledBinary = group.getBinary(fieldName, 0);
////                    if (unscaledBinary.length() == 0) {
////                        yield null;
////                    }
//                    BigInteger unscaledBigInt = new BigInteger(unscaledBinary.getBytesUnsafe());
//                    yield new BigDecimal(unscaledBigInt, scale).floatValue();
//                }
//                default ->
//                        throw new IllegalArgumentException("Unsupported primitive type for DECIMAL: " + primitiveTypeName);
//            };
//        } else if (targetType == LocalDate.class) {
//            // Handle DATE logical type (INT32 epoch days)
//            if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
//                return switch (primitiveTypeName) {
//                    case INT32 -> LocalDate.ofEpochDay(group.getInteger(fieldName, 0));
//                    default ->
//                            throw new IllegalArgumentException("DATE logical type should be stored as INT32. Found: " + primitiveTypeName);
//                };
//            }
//            // Handle TIMESTAMP -> LocalDate conversion (extract date part from timestamp)
//            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType &&
//                    primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
//                long epochMillis = group.getLong(fieldName, 0);
//                return Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of("UTC")).toLocalDate();
//            }
//            // Handle raw INT64 timestamp without logical type annotation -> LocalDate
//            if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64 && logicalType == null) {
//                long epochMillis = group.getLong(fieldName, 0);
//                return Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of("UTC")).toLocalDate();
//            }
//            throw new IllegalArgumentException("Unsupported type for LocalDate: " + primitiveType + " with logical type: " + logicalType);
//        } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeType && targetType == LocalTime.class) {
//            if (timeType.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
//                return switch (primitiveTypeName) {
//                    case INT32 -> LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(group.getInteger(fieldName, 0)));
//                    default ->
//                            throw new IllegalArgumentException("TIME(MILLIS) logical type should be stored as INT32. Found: " + primitiveTypeName);
//                };
//            } else {
//                throw new IllegalArgumentException("Unsupported TIME unit: " + timeType.getUnit() + ". Only MILLIS is supported for LocalTime with INT32.");
//            }
//        } else if (targetType == LocalDateTime.class) {
//            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsType &&
//                    tsType.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS &&
//                    primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
//                return DateUtil.convertEpochMiliSecToLocalDateTime(group.getLong(fieldName, 0), ZoneId.of("UTC"));
//            } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64 && logicalType == null) {
//                return DateUtil.convertEpochMiliSecToLocalDateTime(group.getLong(fieldName, 0), ZoneId.of("UTC"));
//            } else {
//                throw new IllegalArgumentException("Unsupported type for LocalDateTime: " + primitiveType + " with logical type: " + logicalType);
//            }
//        } else if (targetType == Character.class) {
//            return switch (primitiveTypeName) {
//                case BINARY -> {
//                    String value = group.getBinary(fieldName, 0).toStringUsingUTF8();
//                    if (value.length() > 1) {
//                        throw new IllegalArgumentException("Cannot convert BINARY string of length " + value.length() + " to Character. Expected length 1 or empty.");
//                    }
//                    yield value.length() == 1 ? value.charAt(0) : null; // Handle empty string as null
//                }
//                default ->
//                        throw new IllegalArgumentException("Unsupported primitive type for Character: " + primitiveTypeName + ". Expected BINARY.");
//            };
//        } else {
////            if (logicalType == LogicalTypeAnnotation.stringType() && primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY && targetType == String.class) {
////                return group.getBinary(fieldName, 0).toStringUsingUTF8();
////            }
//            return switch (primitiveTypeName) {
//                case INT32 -> group.getInteger(fieldName, 0);
//                case INT64 -> group.getLong(fieldName, 0);
//                case FLOAT -> group.getFloat(fieldName, 0);
//                case DOUBLE -> group.getDouble(fieldName, 0);
//                case BOOLEAN -> group.getBoolean(fieldName, 0);
//                case BINARY -> group.getBinary(fieldName, 0).toStringUsingUTF8();
//                default -> {
//                    try {
//                        yield group.getString(fieldName, 0);
//                    } catch (Exception e) {
//                        throw new IllegalArgumentException("Unsupported primitive type: " + primitiveTypeName +
//                                " and failed to convert to String.", e);
//                    }
//                }
//            };
//        }
//    }

}
