package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.etl.parquet.data.DataAnnotationHelper;
import io.github.veerakumarak.fp.Result;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public class MessageTypeConverter {

    // Static type handlers map for better performance (created once)
    private static final Map<Class<?>, BiConsumer<String, Types.MessageTypeBuilder>> TYPE_HANDLERS = Map.ofEntries(
            Map.entry(int.class, (name, b) -> b.required(PrimitiveType.PrimitiveTypeName.INT32).named(name)),
            Map.entry(long.class, (name, b) -> b.required(PrimitiveType.PrimitiveTypeName.INT64).named(name)),
            Map.entry(double.class, (name, b) -> b.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name)),
            Map.entry(float.class, (name, b) -> b.required(PrimitiveType.PrimitiveTypeName.FLOAT).named(name)),
            Map.entry(boolean.class, (name, b) -> b.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name)),
            Map.entry(char.class, (name, b) -> b.required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name)),
            Map.entry(String.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name)),
            Map.entry(Integer.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name)),
            Map.entry(Long.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name)),
            Map.entry(Double.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name)),
            Map.entry(Float.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name)),
            Map.entry(Boolean.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name)),
            Map.entry(Character.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name)),
            Map.entry(LocalDateTime.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(name)),
            Map.entry(LocalTime.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(name)),
            Map.entry(LocalDate.class, (name, b) -> b.optional(PrimitiveType.PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named(name))
    );

    public static MessageType fromClass(Class<?> tClass) {
        Types.MessageTypeBuilder builder = Types.buildMessage();

        for (Field field : tClass.getDeclaredFields()) {
            // Skip static fields (e.g., constants) and synthetic fields (e.g., $jacocoData from code coverage)
            if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                continue;
            }
            String name = DataAnnotationHelper.getName(field);
            Class<?> type = field.getType();
            BiConsumer<String, Types.MessageTypeBuilder> handler = TYPE_HANDLERS.get(type);
            if (Objects.isNull(handler)) {
                throw new IllegalArgumentException("Unsupported record component type: " + type.getName());
            }
            handler.accept(name, builder);
        }
        return builder.named(tClass.getSimpleName());
    }

    public static Result<MessageType> fromResultSet(String tableName, ResultSetMetaData metaData) {
        return Result.of(() -> {
            int columnCount = metaData.getColumnCount();

            Types.MessageTypeBuilder builder = Types.buildMessage();
            for (int i = 1; i <= columnCount; i++) {
                // Use getColumnLabel() to get the alias (AS name) from SELECT queries
                // This ensures we use the alias (e.g., "PrgsvcID") instead of original column name (e.g., "prgsvcid")
                String columnName = metaData.getColumnLabel(i);
                int columnType = SqlTypeInferrer.getEffectiveType(metaData, i).orElseThrow();
                int precision = metaData.getPrecision(i);
                int scale = metaData.getScale(i);
                int nullable = metaData.isNullable(i);

                switch (columnType) {
                    case java.sql.Types.BOOLEAN:
                    case java.sql.Types.BIT:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                                : Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN))
                                .named(columnName));
                        break;
                    case java.sql.Types.BIGINT:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                : Types.optional(PrimitiveType.PrimitiveTypeName.INT64))
                                .named(columnName));
                        break;
                    case java.sql.Types.INTEGER:
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.TINYINT:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                : Types.optional(PrimitiveType.PrimitiveTypeName.INT32))
                                .named(columnName));
                        break;
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.REAL:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.FLOAT)
                                : Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT))
                                .named(columnName));
                        break;
                    case java.sql.Types.DOUBLE:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                                : Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE))
                                .named(columnName));
                        break;
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.NVARCHAR:
                    case java.sql.Types.LONGVARCHAR:
                    case java.sql.Types.LONGNVARCHAR:
                    case java.sql.Types.CHAR:
                    case java.sql.Types.NCHAR:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                : Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType()))
                                .named(columnName));
                        break;
                    case java.sql.Types.BLOB:
                    case java.sql.Types.BINARY:
                    case java.sql.Types.CLOB:
                    case java.sql.Types.NCLOB:
                    case java.sql.Types.VARBINARY:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                : Types.optional(PrimitiveType.PrimitiveTypeName.BINARY))
                                .named(columnName));
                        break;
                    case java.sql.Types.DATE:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.dateType())
                                : Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.dateType()))
                                .named(columnName));
                        break;
                    case java.sql.Types.TIME:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.timeType(true,
                                        LogicalTypeAnnotation.TimeUnit.MILLIS))
                                : Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.timeType(true,
                                        LogicalTypeAnnotation.TimeUnit.MILLIS)))
                                .named(columnName));
                        break;
                    case java.sql.Types.TIMESTAMP:
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.INT64).as(
                                LogicalTypeAnnotation.timestampType(false,
                                        LogicalTypeAnnotation.TimeUnit.MILLIS))
                                : Types.optional(PrimitiveType.PrimitiveTypeName.INT64).as(
                                LogicalTypeAnnotation.timestampType(false,
                                        LogicalTypeAnnotation.TimeUnit.MILLIS)))
                                .named(columnName));
                        break;
                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.NUMERIC:
                        // PRECISION <= 18 can be transported as LONG derived from an unscaled
                        // BigInteger
                        // PRECISION > 18 must be transported as Binary based on the unscaled
                        // BigInteger's bytes
                        if (scale > 0 && precision <= 18) {
                            builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                    ? Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                    .as(LogicalTypeAnnotation.decimalType(scale, precision))
                                    : Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                    .as(LogicalTypeAnnotation.decimalType(scale, precision)))
                                    .named(columnName));
                        } else if (scale > 0) {
                            builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                    ? Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                    .as(LogicalTypeAnnotation.decimalType(scale, precision))
                                    : Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                    .as(LogicalTypeAnnotation.decimalType(scale, precision)))
                                    .named(columnName));
                        } else if (precision < 5) {
                            builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                    ? Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                    : Types.optional(PrimitiveType.PrimitiveTypeName.INT32))
                                    .named(columnName));
                        } else {
                            builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                    ? Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                    : Types.optional(PrimitiveType.PrimitiveTypeName.INT64))
                                    .named(columnName));
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported SQL type: " + columnType);
                }
            }
            return builder.named(tableName);
        });
    }
}
