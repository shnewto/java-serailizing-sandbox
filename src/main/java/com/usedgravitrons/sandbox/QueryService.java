package com.usedgravitrons.sandbox;

import com.google.cloud.bigquery.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.Map;

@Service
public class QueryService {

    private final BigQuery bigQuery;

    @Autowired
    public QueryService(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
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
