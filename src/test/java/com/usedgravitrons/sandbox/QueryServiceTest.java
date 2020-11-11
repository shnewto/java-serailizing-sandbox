package com.usedgravitrons.sandbox;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@SpringBootTest(properties = "tableName=github_timeline_${random.int[1,10000]}")
@SpringBootTest(properties = "tableName=github_timeline")
class QueryServiceTest {
    @Autowired
    BigQuery bigQuery;

    @Value("classpath:data/github_timeline.csv")
    Resource csvFile;

    @Autowired
    QueryService queryService;

    @Value("${spring.cloud.gcp.bigquery.datasetName}")
    String datasetName;
    @Value("${tableName}")
    String tableName;

//    @BeforeAll
//    void setUp() throws IOException, ExecutionException, InterruptedException {
//        Job _job = queryService.loadData(csvFile.getInputStream(), tableName);
//    }
//
//    @AfterAll
//    void tearDown() {
//        bigQuery.delete(tableName);
//    }

    @Test
    void runQuery() throws InterruptedException {
        String query = String.format("" +
                "SELECT repository_name, repository_owner FROM %s.%s " +
                "WHERE repository_open_issues > 10 AND repository_watchers > 10 " +
                "GROUP BY repository_name, repository_owner;", datasetName, tableName);

        TableResult tableResult = queryService.runQuery(query);

        Map<String, String> result = queryService.tableResultToMap(tableResult);
        assertThat(result.size()).isEqualTo(4936);
        assertThat(result.get("ember.js")).isEqualTo("emberjs");
        assertThat(result.get("request")).isEqualTo("mikeal");
        assertThat(result.get("droid-fu")).isEqualTo("kaeppler");
        assertThat(result.get("v2ex")).isEqualTo("livid");
        assertThat(result.get("arcemu")).isEqualTo("arcemu");
        assertThat(result.get("fancyBox")).isEqualTo("fancyapps");
    }
}