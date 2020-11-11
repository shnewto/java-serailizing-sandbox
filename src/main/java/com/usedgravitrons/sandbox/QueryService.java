package com.usedgravitrons.sandbox;

import com.google.cloud.bigquery.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.bigquery.core.BigQueryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class QueryService {

    @Autowired
    private BigQueryTemplate bigQueryTemplate;

    @Autowired
    private BigQuery bigQuery;

    public Job loadData(InputStream dataInputStream, String tableName) throws ExecutionException, InterruptedException {
        ListenableFuture<Job> bigQueryJobFuture =
                bigQueryTemplate.writeDataToTable(
                        tableName,
                        dataInputStream,
                        FormatOptions.csv()
                );

        return bigQueryJobFuture.get();
    }

    public TableResult runQuery(String query) throws InterruptedException {
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(query).build();
        return bigQuery.query(queryJobConfiguration);
    }

    public Map<String, String> tableResultToMap(TableResult tableResult) {
        Map<String, String> result = new HashMap<>();
        for (FieldValueList row : tableResult.iterateAll()) {
            if (row.size() == 2) {
                result.put(row.get(0).getStringValue(), row.get(1).getStringValue());
            }
        }

        return result;
    }
}
