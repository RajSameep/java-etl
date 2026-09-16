package io.github.veerakumarak.etl.sink;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.etl.parquet.ParquetAwsManager;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InternalFailure;
import io.github.veerakumarak.fp.failures.InvalidRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

public class CsvWriterHelper {

    private static final Logger log = LoggerFactory.getLogger(CsvWriterHelper.class);

    protected static Result<FileMetaData> writeBatched(String writePath, String tableName, ResultSet resultSet, Integer batchSize, List<String> partitionKeys) {
        return Result.of(() -> {
            if (Objects.isNull(resultSet)) {
                throw new InternalFailure("ResultSet is null");
            }
            if (batchSize == null || batchSize <= 0) {
                throw new InvalidRequest("Batch size must be positive");
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Build CSV header
            StringBuilder header = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) header.append(",");
                header.append(escapeCsv(metaData.getColumnLabel(i)));
            }

            // Group rows by partition
            // For DQC extracts, partitionKeys is typically empty, so all rows go to "default"
            Map<String, List<String>> partitionRows = new LinkedHashMap<>();
            Map<String, Long> partitionCounts = new HashMap<>();

            while (resultSet.next()) {
                String partitionPrefix = getPartitionPrefix(metaData, resultSet, partitionKeys);

                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) row.append(",");
                    Object value = resultSet.getObject(i);
                    row.append(value == null ? "" : escapeCsv(value.toString()));
                }

                partitionRows.computeIfAbsent(partitionPrefix, k -> new ArrayList<>()).add(row.toString());
                partitionCounts.merge(partitionPrefix, 1L, Long::sum);
            }

            // Write CSV files to S3/local using Hadoop FileSystem
            Configuration conf = ParquetAwsManager.getConfiguration();
            Set<String> generatedFiles = new HashSet<>();

            for (Map.Entry<String, List<String>> entry : partitionRows.entrySet()) {
                String partitionPrefix = entry.getKey();
                List<String> rows = entry.getValue();
                String filePath = getFilePath(writePath, partitionPrefix, tableName);

                Path hadoopPath = new Path(filePath);
                FileSystem fs = hadoopPath.getFileSystem(conf);

                try (FSDataOutputStream outputStream = fs.create(hadoopPath, true);
                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {

                    writer.write(header.toString());
                    writer.newLine();

                    for (String row : rows) {
                        writer.write(row);
                        writer.newLine();
                    }
                }

                generatedFiles.add(filePath);
                log.info("CSV file written: {} ({} rows)", filePath, rows.size());
            }

            log.info("CSV write complete. Total partitions: {}", partitionRows.size());
            return new FileMetaData(writePath, partitionCounts, generatedFiles);
        });
    }

    private static String getFilePath(String writePath, String partitionPrefix, String tableName) {
        return String.format("%s/%s/%s.csv", writePath, partitionPrefix, tableName);
    }

    private static String getPartitionPrefix(ResultSetMetaData metaData, ResultSet resultSet, List<String> partitionKeys) {
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return "default";
        }
        try {
            StringBuilder prefix = new StringBuilder();
            for (String key : partitionKeys) {
                int colIndex = findColumnIndex(metaData, key);
                if (colIndex > 0) {
                    if (!prefix.isEmpty()) prefix.append("/");
                    Object value = resultSet.getObject(colIndex);
                    prefix.append(key).append("=").append(value == null ? "__null__" : value.toString());
                }
            }
            return prefix.isEmpty() ? "default" : prefix.toString();
        } catch (Exception e) {
            log.warn("Failed to compute partition prefix, using 'default': {}", e.getMessage());
            return "default";
        }
    }

    private static int findColumnIndex(ResultSetMetaData metaData, String columnName) {
        try {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (metaData.getColumnLabel(i).equalsIgnoreCase(columnName)) {
                    return i;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to find column index for '{}': {}", columnName, e.getMessage());
        }
        return -1;
    }

    private static String escapeCsv(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
