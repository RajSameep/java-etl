package io.github.veerakumarak.etl.sink;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.etl.utils.FileType;
import io.github.veerakumarak.fp.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.List;
import java.util.stream.Stream;

public class DataSink {

    private static final Logger log = LoggerFactory.getLogger(DataSink.class);

    public static Result<FileMetaData> write(String writePath, FileType fileType, String tableName, ResultSet resultSet, Integer batchSize, List<String> partitionKeys) {
        if (fileType == FileType.PARQUET) {
            return ParquetWriterHelper.writeBatched(writePath, tableName, resultSet, batchSize, partitionKeys);
        } else if (fileType == FileType.CSV) {
            return CsvWriterHelper.writeBatched(writePath, tableName, resultSet, batchSize, partitionKeys);
        }
        log.error("Unsupported output path: " + writePath);
        return Result.failure("Unsupported file format provided");
    }

    public static <T> Result<FileMetaData> writeStream(String writePath, FileType fileType, String tableName, Integer batchSize, Stream<T> data, Class<T> tClass, List<String> partitionKeys) {
        if (fileType == FileType.PARQUET) {
            return ParquetWriterHelper.writeBatched(writePath, tableName, batchSize, data, tClass, partitionKeys);
        } else if (fileType == FileType.CSV) {
//            return CsvWriterHelper.writeBatched(writePath, tableName, batchSize, data, tClass, partitionKeys);
        }
        log.error("Unsupported output path: " + writePath);
        return Result.failure("Unsupported file format provided");
    }

}
