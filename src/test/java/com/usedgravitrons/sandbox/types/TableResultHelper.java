package com.usedgravitrons.sandbox.types;

import com.google.api.gax.paging.Page;
import com.google.cloud.PageImpl;
import com.google.cloud.bigquery.*;

import java.util.ArrayList;
import java.util.List;

public class TableResultHelper {
    final SchemaHelper schema;
    final long totalRows;
    List<FieldValueList> fieldValueLists;

    public TableResultHelper(TableResult tableResult) {
        this.schema = new SchemaHelper(tableResult.getSchema());
        this.totalRows = tableResult.getTotalRows();
        fieldValueLists = new ArrayList<>();
        pageNoSchema(tableResult);
    }

    private void sanitizeFieldValueLists() {
        List<FieldValueList> sanitized = new ArrayList<>();
        for (List<FieldValue> values: fieldValueLists) {
            sanitized.add(FieldValueList.of(values, schema.getFields()));
        }
        fieldValueLists = sanitized;
    }

    private void pageNoSchema(TableResult tableResult) {
        tableResult.iterateAll().iterator().forEachRemaining(fieldValueLists::add);
    }

    private Page<FieldValueList> pageNoSchema() {
        return new PageImpl<>(
                (PageImpl.NextPageFetcher<FieldValueList>) () -> null,
                null,
                fieldValueLists);
    }

    public TableResult toTableResult() {
        sanitizeFieldValueLists();
        return new TableResult(schema.toSchema(), totalRows, pageNoSchema());
    }

}

