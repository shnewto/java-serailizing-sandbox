package com.usedgravitrons.sandbox.types;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;

import java.util.List;

public class TestFieldList {
    List<Field> fields;

    TestFieldList(List<Field> fields) {
        this.fields = fields;
    }

    public FieldList toFieldList() {
        return FieldList.of(fields);
    }
}
