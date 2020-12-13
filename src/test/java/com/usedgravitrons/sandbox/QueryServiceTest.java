package com.usedgravitrons.sandbox;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import com.github.shnewto.bqjson.SerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
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

    @Test
    void runAndCompareLiveAndSerialized() throws IOException, InterruptedException {
        TableResult tableResult = queryService.runQuery(testQuery());
        Files.write(Paths.get("tr_002.json"), SerDe.toJsonBytes(tableResult));
        TableResult deserialized = SerDe.fromJson(Files.readAllBytes(Paths.get("tr_002.json")), TableResult.class);
        TableResult actual = queryService.runQuery(testQuery());

        assertThat(deserialized).isEqualToComparingFieldByField(actual);
        assertThat(deserialized).isExactlyInstanceOf(TableResult.class);
    }

    @Test
    void runAndSerializeQuery() throws InterruptedException, IOException {
        TableResult tableResult = queryService.runQuery(testQuery());
        Files.write(Paths.get("tr_001.json"), SerDe.toJsonBytes(tableResult));
    }

    @Test
    void runDeserializedQuery() throws IOException {
        TableResult tableResult = SerDe.fromJson(Files.readAllBytes(Paths.get("tr_001.json")), TableResult.class);
        Map<String, String> result = queryService.tableResultToMap(tableResult);

        assertThat(result.size()).isEqualTo(4936);
        assertThat(result.get("ember.js")).isEqualTo("emberjs");
        assertThat(result.get("request")).isEqualTo("mikeal");
        assertThat(result.get("droid-fu")).isEqualTo("kaeppler");
        assertThat(result.get("v2ex")).isEqualTo("livid");
        assertThat(result.get("arcemu")).isEqualTo("arcemu");
        assertThat(result.get("fancyBox")).isEqualTo("fancyapps");
    }

    @Test
    void runQuery() throws InterruptedException {
        TableResult tableResult = queryService.runQuery(testQuery());
        Map<String, String> result = queryService.tableResultToMap(tableResult);

        assertThat(result.size()).isEqualTo(4936);
        assertThat(result.get("ember.js")).isEqualTo("emberjs");
        assertThat(result.get("request")).isEqualTo("mikeal");
        assertThat(result.get("droid-fu")).isEqualTo("kaeppler");
        assertThat(result.get("v2ex")).isEqualTo("livid");
        assertThat(result.get("arcemu")).isEqualTo("arcemu");
        assertThat(result.get("fancyBox")).isEqualTo("fancyapps");
    }

    private String testQuery() {
        return String.format("" +
                "SELECT repository_name, repository_owner FROM %s.%s " +
                "WHERE repository_open_issues > 10 AND repository_watchers > 10 " +
                "GROUP BY repository_name, repository_owner;", datasetName, tableName);
    }
}