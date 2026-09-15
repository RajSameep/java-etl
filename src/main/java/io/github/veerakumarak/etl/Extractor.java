package io.github.veerakumarak.etl;

import io.github.veerakumarak.etl.entities.ExtractResult;
import io.github.veerakumarak.etl.datasource.IDataSource;
import io.github.veerakumarak.etl.file.GenericFileReader;
import io.github.veerakumarak.etl.sink.DataSink;
import io.github.veerakumarak.etl.utils.FileType;
import io.github.veerakumarak.etl.utils.TemplateUtil;
import io.github.veerakumarak.fp.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class Extractor {

    private static final Logger log = LoggerFactory.getLogger(Extractor.class);

    public static QueryProviderStep job(String jobName, IDataSource dataSource) {
        return new Builder(jobName, dataSource);
    }

    public interface QueryProviderStep {
        ParametersStep withQuery(String sqlPath);
    }

    public interface ParametersStep {
        PartitionKeysStep withParameters(Map<String, String> parameters);
    }

    public interface PartitionKeysStep {
        FetchSizeStep withPartitionKeys(List<String> partitionKeys);
    }

    public interface FetchSizeStep {
        WriterStep withFetchSize(int fetchSize);

        Result<ExtractResult> toCsv(String writePath);

        Result<ExtractResult> toParquet(String writePath);
    }

    public interface WriterStep {
        Result<ExtractResult> toCsv(String writePath);

        Result<ExtractResult> toParquet(String writePath);
    }

    private static class Builder implements QueryProviderStep, ParametersStep, PartitionKeysStep, FetchSizeStep, WriterStep {
        private final String jobName;
        private final IDataSource dataSource;
        private String queryPath;
        private Map<String, String> parameters;
        private List<String> partitionKeys;
        private int fetchSize;
        private String writePath;

        public Builder(String jobName, IDataSource dataSource) {
            this.jobName = jobName;
            this.dataSource = dataSource;
            this.fetchSize = 1;
        }

        @Override
        public ParametersStep withQuery(String queryPath) {
            this.queryPath = queryPath;
            return this;
        }

        @Override
        public PartitionKeysStep withParameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        @Override
        public FetchSizeStep withPartitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        @Override
        public WriterStep withFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        @Override
        public Result<ExtractResult> toParquet(String writePath) {
            this.writePath = writePath;
            return execute(FileType.PARQUET);
        }

        @Override
        public Result<ExtractResult> toCsv(String writePath) {
            this.writePath = writePath;
            return execute(FileType.CSV);
        }

        private Result<ExtractResult> execute(FileType fileType) {
            return GenericFileReader.readFile(queryPath)
                    .map(template -> TemplateUtil.render(template, parameters))
                    .flatMap(query -> Result.of(() -> {
                        try (Connection conn = dataSource.connection().get();
                             PreparedStatement ps = conn.prepareStatement(query)) {

                            ps.setFetchSize(fetchSize);

                            try (ResultSet rs = ps.executeQuery()) {
                                return DataSink.write(writePath, fileType, jobName, rs, fetchSize, partitionKeys)
                                        .map(ExtractResult::new)
                                        .get();
                            }
                        }
                    }));
        }
    }

}