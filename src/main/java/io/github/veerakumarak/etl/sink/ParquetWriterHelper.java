package io.github.veerakumarak.etl.sink;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.etl.parquet.MessageTypeConverter;
import io.github.veerakumarak.etl.parquet.ParquetAwsManager;
import io.github.veerakumarak.etl.parquet.converters.ClassToGroupConverter;
import io.github.veerakumarak.etl.parquet.converters.ResultSetToGroupConverter;
import io.github.veerakumarak.etl.parquet.partitions.ParquetPartitionHelper;
import io.github.veerakumarak.fp.Failure;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InternalFailure;
import io.github.veerakumarak.fp.failures.InvalidRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ParquetWriterHelper {

    private static final Logger log = LoggerFactory.getLogger(ParquetWriterHelper.class);

    private static ParquetWriter<Group> createWriter(String filePath, MessageType schema, Configuration conf) throws IOException {
        return ExampleParquetWriter.builder(new Path(filePath))
                .withConf(conf)
                .withType(schema)
                .withDictionaryEncoding(true)
                .withValidation(false)
                .withPageWriteChecksumEnabled(false)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    private static MessageType excludeFields(MessageType schema, List<String> fieldNames) {
        if (fieldNames == null || fieldNames.isEmpty()) return schema;
        return new MessageType(schema.getName(),
                schema.getFields().stream()
                        .filter(f -> !fieldNames.contains(f.getName()))
                        .collect(Collectors.toList()));
    }

    private static Group projectGroup(Group source, MessageType sourceSchema, MessageType targetSchema, SimpleGroupFactory factory) {
        if (sourceSchema.getFieldCount() == targetSchema.getFieldCount()) return source;
        Group target = factory.newGroup();
        for (int i = 0; i < targetSchema.getFieldCount(); i++) {
            String name = targetSchema.getFieldName(i);
            int srcIdx = sourceSchema.getFieldIndex(name);
            if (source.getFieldRepetitionCount(srcIdx) == 0) continue;
            PrimitiveType.PrimitiveTypeName type = sourceSchema.getType(srcIdx).asPrimitiveType().getPrimitiveTypeName();
            if (type == PrimitiveType.PrimitiveTypeName.BINARY) {
                target.add(name, source.getBinary(srcIdx, 0));
            } else if (type == PrimitiveType.PrimitiveTypeName.INT32) {
                target.add(name, source.getInteger(srcIdx, 0));
            } else if (type == PrimitiveType.PrimitiveTypeName.INT64) {
                target.add(name, source.getLong(srcIdx, 0));
            } else if (type == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                target.add(name, source.getDouble(srcIdx, 0));
            } else if (type == PrimitiveType.PrimitiveTypeName.FLOAT) {
                target.add(name, source.getFloat(srcIdx, 0));
            } else if (type == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                target.add(name, source.getBoolean(srcIdx, 0));
            }
        }
        return target;
    }

    protected static Result<FileMetaData> writeBatched(String writePath, String tableName, ResultSet resultSet, Integer batchSize, List<String> partitionKeys) {

        return Result.of(() -> {
            if (Objects.isNull(resultSet)) {
                throw new InternalFailure("ResultSet is null");
            }
            if (batchSize == null || batchSize <= 0) {
                throw new InvalidRequest("Batch size must be positive");
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            MessageType schema = MessageTypeConverter.fromResultSet(tableName, metaData)
                    .orElseThrow(() -> new InternalFailure("Could not get schema from result set"));

            MessageType writeSchema = excludeFields(schema, partitionKeys);
            SimpleGroupFactory writeGroupFactory = new SimpleGroupFactory(writeSchema);

            Map<String, ParquetWriter<Group>> writers = new HashMap<>();
            Map<String, Long> partitionCounts = new HashMap<>();
            List<Group> batch = new ArrayList<>(batchSize);
            while (resultSet.next()) {
                Group group = ResultSetToGroupConverter.convert(schema, metaData, resultSet)
                        .orElseThrow(() -> new InternalFailure("Could not get group from result set"));
                batch.add(group);
                if (batch.size() >= batchSize) {
                    for (Group g : batch) {
                        write(schema, writeSchema, writeGroupFactory, g, writers, partitionCounts, writePath, tableName, partitionKeys).orThrow();
                    }
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                for (Group g : batch) {
                    write(schema, writeSchema, writeGroupFactory, g, writers, partitionCounts, writePath, tableName, partitionKeys).orThrow();
                }
            }
            for (ParquetWriter<Group> writer : writers.values()) {
                writer.close();
            }
            log.info("Batched write from resultset complete.");
            return new FileMetaData(writePath,
                    partitionCounts,
                    partitionCounts.entrySet().stream()
                            .map(entry -> getFilePath(writePath, entry.getKey(), tableName))
                            .collect(Collectors.toSet()));
        });
    }

    private static String getFilePath(String writePath, String partitionPrefix, String tableName) {
        return String.format("%s/%s/%s.parquet", writePath, partitionPrefix, tableName);
    }

    private static Failure write(MessageType schema, MessageType writeSchema, SimpleGroupFactory writeGroupFactory,
                                 Group group, Map<String, ParquetWriter<Group>> writers,
                                 Map<String, Long> partitionCounts, String writePath,
                                 String tableName, List<String> partitionKeys) {
        return Failure.of(() -> {
            String partitionPrefix = ParquetPartitionHelper.getPartitionPrefix(schema, partitionKeys, group).orElseThrow();
            ParquetWriter<Group> writer = writers.computeIfAbsent(partitionPrefix, val -> {
                String filePath = getFilePath(writePath, partitionPrefix, tableName);
                try {
                    return createWriter(filePath, writeSchema, ParquetAwsManager.getConfiguration());
                } catch (IOException e) {
                    throw new InternalFailure(e.getMessage());
                }
            });
            partitionCounts.merge(partitionPrefix, 1L, Long::sum);
            writer.write(projectGroup(group, schema, writeSchema, writeGroupFactory));
        });
    }

    protected static <T> Result<FileMetaData> writeBatched(String writePath, String tableName, Integer batchSize, Stream<T> tStream, Class<T> tClass, List<String> partitionKeys) {
        return Result.of(() -> {
            if (Objects.isNull(tStream)) {
                throw new InternalFailure("Data stream is null");
            }
            if (batchSize == null || batchSize <= 0) {
                throw new InvalidRequest("Batch size must be positive");
            }

            MessageType schema = MessageTypeConverter.fromClass(tClass);
            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

            MessageType writeSchema = excludeFields(schema, partitionKeys);
            SimpleGroupFactory writeGroupFactory = new SimpleGroupFactory(writeSchema);

            Map<String, ParquetWriter<Group>> writers = new HashMap<>();
            Map<String, Long> partitionCounts = new HashMap<>();

            List<Group> batch = new ArrayList<>(batchSize);
            Iterator<T> iterator = tStream.iterator();
            while (iterator.hasNext()) {
                T tDatum = iterator.next();
                batch.add(ClassToGroupConverter.toGroup(tDatum, groupFactory));
                if (batch.size() >= batchSize) {
                    for (Group g : batch) {
                        write(schema, writeSchema, writeGroupFactory, g, writers, partitionCounts, writePath, tableName, partitionKeys).orThrow();
                    }
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                for (Group g : batch) {
                    write(schema, writeSchema, writeGroupFactory, g, writers, partitionCounts, writePath, tableName, partitionKeys).orThrow();
                }
            }

            for (ParquetWriter<Group> writer : writers.values()) {
                writer.close();
            }
            log.info("Batched write complete");
            return new FileMetaData(writePath,
                    partitionCounts,
                    partitionCounts.entrySet().stream()
                            .map(entry -> getFilePath(writePath, entry.getKey(), tableName))
                            .collect(Collectors.toSet()));
        });
    }
}
