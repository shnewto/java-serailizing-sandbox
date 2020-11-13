package com.usedgravitrons.sandbox.types;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;

public class TestSchema {
    TestFieldList fields;
    TestSchema(Schema schema) {
        this.fields = new TestFieldList(schema.getFields());
    }

    public Schema toSchema() {
        return Schema.of(fields.toFieldList());
    }

    public FieldList getFields() {
        return fields.toFieldList();
    }
}
