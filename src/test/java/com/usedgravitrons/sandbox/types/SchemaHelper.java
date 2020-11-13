package com.usedgravitrons.sandbox.types;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;

public class SchemaHelper {
    public Schema toSchema() {
        return Schema.of(fields.toFieldList());
    }
    public FieldList getFields() {
        return fields.toFieldList();
    }

    final FieldListHelper fields;
    SchemaHelper(Schema schema) {
        this.fields = new FieldListHelper(schema.getFields());
    }
}
