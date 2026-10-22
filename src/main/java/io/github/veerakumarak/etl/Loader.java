package io.github.veerakumarak.etl;

import io.github.veerakumarak.etl.datasource.IDataSource;
import io.github.veerakumarak.etl.entities.LoadResult;
import io.github.veerakumarak.etl.parquet.ParquetDataBaseHelper;
import io.github.veerakumarak.etl.utils.RegexUtil;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InvalidRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.time.ZoneId;

public class Loader {

    private static final Logger log = LoggerFactory.getLogger(Loader.class);

    public static TableNameStep job(String jobName, IDataSource dataSource) {
        return new Builder(jobName, dataSource);
    }

    public interface TableNameStep {
        TruncateStep withTableName(String tableName);
    }

    public interface TruncateStep {
        BatchSizeStep truncate(Boolean truncate);
    }

    public interface BatchSizeStep {
        WriterStep withBatchSize(int batchSize);
    }

    public interface WriterStep {
        Result<LoadResult> fromParquet(String writePath);

        /**
         * Loads from a Parquet file, interpreting timestamp columns (INT64 and legacy INT96) in the
         * given time zone.
         */
        Result<LoadResult> fromParquet(String writePath, ZoneId zoneId);
    }

    private static class Builder implements TableNameStep, TruncateStep, BatchSizeStep, WriterStep {
        private final String jobName;
        private final IDataSource dataSource;
        private String tableName;
        private boolean truncate;
        private int batchSize;
        private String readPath;
        private ZoneId zoneId = ZoneId.systemDefault();

        public Builder(String jobName, IDataSource dataSource) {
            this.jobName = jobName;
            this.dataSource = dataSource;
        }

        @Override
        public TruncateStep withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        @Override
        public BatchSizeStep truncate(Boolean truncate) {
            this.truncate = truncate;
            return this;
        }

        @Override
        public WriterStep withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        @Override
        public Result<LoadResult> fromParquet(String readPath) {
            this.readPath = readPath;
            return execute();
        }

        @Override
        public Result<LoadResult> fromParquet(String readPath, ZoneId zoneId) {
            this.readPath = readPath;
            this.zoneId = zoneId != null ? zoneId : ZoneId.systemDefault();
            return execute();
        }

        private Result<LoadResult> execute() {
            return Result.of(() -> {
                if (!RegexUtil.matches("^[a-zA-Z_][a-zA-Z0-9_$]*$", tableName)) {
                    throw new InvalidRequest("Invalid Table Name: " + tableName);
                }

                try (Connection conn = dataSource.connection().get()) {
                    conn.setAutoCommit(false); // Start transaction
                    try {
                        if (truncate) {
                            log.info("Truncating table: {} before inserting.", tableName);
                            dataSource.truncate(conn, tableName).orThrow();
                        }

                        // Pass the existing connection to the reader helper
                        long count = ParquetDataBaseHelper.writeBatched(
                                readPath, conn, tableName, batchSize, zoneId
                        ).orElseThrow();

                        conn.commit(); // Only commit if everything worked
                        return new LoadResult(count);

                    } catch (Exception e) {
                        conn.rollback(); // Undo truncate/partial loads on error
                        throw e;
                    }
                }
            });
        }

    }

}