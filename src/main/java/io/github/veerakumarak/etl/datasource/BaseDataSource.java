package io.github.veerakumarak.etl.datasource;

import io.github.veerakumarak.etl.utils.RegexUtil;
import io.github.veerakumarak.fp.Failure;
import io.github.veerakumarak.fp.Pair;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InvalidRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public abstract class BaseDataSource implements IDataSource {

    private static final Logger log = LoggerFactory.getLogger(BaseDataSource.class);

    protected String url;
    protected String user;
    protected String password;

    public BaseDataSource(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public Result<ResultSetMetaData> getSchema(Connection connection, String tableName) {
        return Result.of(() -> {
            String query = "SELECT * FROM " + tableName + " LIMIT 0";

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                return rs.getMetaData();
            }
        });
    }

    @Override
    public Result<ResultSet> read(Connection connection, String sql) {
        return read(connection, sql, null);
    }

    @Override
    public Result<ResultSet> read(Connection connection, String sql, Integer fetchSize) {
        return Result.of(() -> {
            boolean streamingMode = isValidFetchSize(fetchSize);
            
            if (streamingMode) {
                configureForStreaming(connection);
            }
            
            PreparedStatement stmt = connection.prepareStatement(
                    sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
            );
            
            if (streamingMode) {
                stmt.setFetchSize(fetchSize);
                log.info("Streaming mode enabled with fetch size: {}", fetchSize);
            }
            
            log.info("Executing query...");
            long startTime = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery();
            log.info("Query executed in {}ms", System.currentTimeMillis() - startTime);
            return rs;
        });
    }

    /**
     * Checks if the fetch size is valid for streaming mode.
     */
    private boolean isValidFetchSize(Integer fetchSize) {
        return fetchSize != null && fetchSize > 0;
    }

    /**
     * Configures the connection for streaming mode.
     * PostgreSQL requires autoCommit to be disabled for streaming.
     */
    private void configureForStreaming(Connection connection) throws SQLException {
        String dbProduct = connection.getMetaData().getDatabaseProductName().toLowerCase();
        if (dbProduct.contains("postgres") && connection.getAutoCommit()) {
            log.debug("Disabling autoCommit for PostgreSQL streaming");
            connection.setAutoCommit(false);
        }
    }

    @Override
    public Result<Long> insertBatch(Connection connection, String sql, List<List<Pair<Object, Integer>>> batchValuesAndTypes, Integer batchSize) {
        if (batchValuesAndTypes == null || batchValuesAndTypes.isEmpty()) {
            return Result.ok(0L);
        }

        if (batchSize <= 0) {
            return Result.failure(new InvalidRequest("Invalid batch size"));
        }

        int totalRows = batchValuesAndTypes.size();
        log.info("Starting batch insert. Total rows to process: {}, Batch size: {}", totalRows, batchSize);


        return Result.of(() -> {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            long successCount = 0, batchNumber = 0;

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                for (int startIndex = 0; startIndex < totalRows; startIndex += batchSize) {
                    batchNumber++;
                    int endIndex = Math.min(startIndex + batchSize, totalRows);

                    for (int i = startIndex; i < endIndex; i++) {
                        List<Pair<Object, Integer>> valuesAndTypes = batchValuesAndTypes.get(i);
                        for (int j = 0; j < valuesAndTypes.size(); j++) {
                            Object value = valuesAndTypes.get(j).getFirst();
                            Integer type = valuesAndTypes.get(j).getSecond();
                            stmt.setObject(j + 1, value, type);
                        }
                        stmt.addBatch();
                    }

                    int[] results = stmt.executeBatch();
                    long batchSuccessCount = Arrays.stream(results)
                            .filter(r -> r >= 0 || r == Statement.SUCCESS_NO_INFO)
                            .count();
                    successCount += batchSuccessCount;

                    connection.commit();
                    stmt.clearBatch();

                    log.info("Processed batch {} of {}. Rows in batch: {}, Cumulative success count: {}",
                            batchNumber,
                            (int) Math.ceil((double) totalRows / batchSize),
                            results.length,
                            successCount);
                }

                log.info("Successfully completed batch insert. Total rows inserted: {}", successCount);
                return successCount;
            } catch (SQLException e) {
                log.error("Error during batch insert on batch number {}. Rolling back transaction.", batchNumber, e);
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        });
    }

    @Override
    public Failure truncate(Connection connection, String tableName) {
        return Failure.of(() -> {
            if(validateTableName(tableName)){
                String query = "TRUNCATE TABLE " + tableName;
                try(Statement stmt = connection.createStatement()){
                    stmt.executeUpdate(query);
                }
            }else{
                throw new IllegalArgumentException("Invalid tableName");
            }
        });
    }

    private boolean validateTableName(String tableName){
        return RegexUtil.matches("^[a-zA-Z_][a-zA-Z0-9_$]*$", tableName);
    }

    @Override
    public String getUrl() {
        return url;
    }

}
