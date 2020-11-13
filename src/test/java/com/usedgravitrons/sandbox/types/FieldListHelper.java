package com.usedgravitrons.sandbox.types;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;

import java.util.List;

public class FieldListHelper {
    final List<Field> fields;

    FieldListHelper(List<Field> fields) {
        this.fields = fields;
    }

    public FieldList toFieldList() {
        return FieldList.of(fields);
    }
}
