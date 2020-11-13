package com.usedgravitrons.sandbox;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.usedgravitrons.sandbox.types.TableResultHelper;
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
//@SpringBootTest(properties = "tableName=github_timeline_${random.int[1,10000]}")
@SpringBootTest(properties = "tableName=github_timeline")
class QueryServiceTest {

    private final Gson gson = new Gson();

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
        serializeQuery(tableResult, "tr_002.json");
        TableResult deserialized = deserializeQuery("tr_002.json");
        TableResult actual = queryService.runQuery(testQuery());

        assertThat(deserialized).isEqualToComparingFieldByField(actual);
        assertThat(deserialized).isExactlyInstanceOf(TableResult.class);
    }

    @Test
    void runAndSerializeQuery() throws InterruptedException, IOException {
        TableResult tableResult = queryService.runQuery(testQuery());
        serializeQuery(tableResult, "tr_001.json");
    }

    @Test
    void runDeserializedQuery() throws IOException {
        TableResult tableResult = deserializeQuery("tr_001.json");
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

    void serializeQuery(TableResult tableResult, String fpath) throws IOException {
        TableResultHelper tableResultHelper = new TableResultHelper(tableResult);
        Files.write(Paths.get(fpath), gson.toJson(tableResultHelper).getBytes());
    }

    TableResult deserializeQuery(String fpath) throws IOException {
        TableResultHelper tableResultHelper = gson.fromJson(new String(Files.readAllBytes(Paths.get(fpath))), TableResultHelper.class);
        return tableResultHelper.toTableResult();
    }

    private String testQuery() {
        return String.format("" +
                "SELECT repository_name, repository_owner FROM %s.%s " +
                "WHERE repository_open_issues > 10 AND repository_watchers > 10 " +
                "GROUP BY repository_name, repository_owner;", datasetName, tableName);
    }
}