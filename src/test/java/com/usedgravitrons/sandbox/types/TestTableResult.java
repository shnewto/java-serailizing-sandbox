package com.usedgravitrons.sandbox.types;

import com.google.api.gax.paging.Page;
import com.google.cloud.PageImpl;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class TestTableResult {
    TestSchema schema;
    long totalRows;
    List<FieldValueList> fieldValueLists;

    public TestTableResult(TableResult tableResult) {
        this.schema = new TestSchema(tableResult.getSchema());
        this.totalRows = tableResult.getTotalRows();
        fieldValueLists = new ArrayList<FieldValueList>();
        pageNoSchema(tableResult);
    }

    private void sanitizeFieldValueLists() {
        List<FieldValueList> sanitized = new ArrayList<FieldValueList>();
        for (List<FieldValue> values: fieldValueLists) {
            sanitized.add(FieldValueList.of(values, schema.getFields()));
        }
        fieldValueLists = sanitized;
    }

    private void pageNoSchema(TableResult tableResult) {
        tableResult.iterateAll().iterator().forEachRemaining(fieldValueLists::add);
    }

    private Page<FieldValueList> pageNoSchema() {
                return new PageImpl<FieldValueList>(
                        (PageImpl.NextPageFetcher<FieldValueList>) () -> null,
                        null,
                        fieldValueLists);
    }

    public TableResult toTableResult() {
        sanitizeFieldValueLists();
        return new TableResult(schema.toSchema(), totalRows, pageNoSchema());
    }

}

