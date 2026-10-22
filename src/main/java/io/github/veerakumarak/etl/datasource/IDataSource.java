package io.github.veerakumarak.etl.datasource;

import io.github.veerakumarak.fp.Failure;
import io.github.veerakumarak.fp.Pair;
import io.github.veerakumarak.fp.Result;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Properties;

public interface IDataSource {
    String getUrl();
    Properties getProperties();

    default Result<Connection> connection() {
        Properties props = getProperties();
        return Result.of(() -> DriverManager.getConnection(getUrl(), props.getProperty("user"), props.getProperty("password")));
    }

    Result<ResultSetMetaData> getSchema(Connection connection, String tableName);
    Result<ResultSet> read(Connection connection, String sql);
    Result<ResultSet> read(Connection connection, String sql, Integer fetchSize);
    Failure truncate(Connection connection, String tableName);
    Result<Long> insertBatch(Connection connection, String sql, List<List<Pair<Object, Integer>>> batchValues, Integer batchSize);
}
