package org.fluentjdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseUpdateBuilder extends DatabaseStatement {

    private final String tableName;
    private final List<String> whereConditions = new ArrayList<>();
    private final List<Object> whereParameters = new ArrayList<>();
    private final List<String> updateFields = new ArrayList<>();
    private final List<Object> updateValues = new ArrayList<>();

    public DatabaseUpdateBuilder(String tableName) {
        this.tableName = tableName;
    }

    DatabaseUpdateBuilder setWhereFields(List<String> whereConditions, List<Object> whereParameters) {
        this.whereConditions.addAll(whereConditions);
        this.whereParameters.addAll(whereParameters);
        return this;
    }

    public DatabaseUpdateBuilder setFields(List<String> fields, List<Object> values) {
        this.updateFields.addAll(fields);
        this.updateValues.addAll(values);
        return this;
    }

    public DatabaseUpdateBuilder setField(String field, @Nullable Object value) {
        this.updateFields.add(field);
        this.updateValues.add(value);
        return this;
    }

    public DatabaseUpdateBuilder setFieldIfPresent(String field, @Nullable Object value) {
        if (value != null) {
            setField(field, value);
        }
        return this;
    }


    public void execute(Connection connection) {
        if (updateFields.isEmpty()) {
            return;
        }
        List<Object> parameters = new ArrayList<>();
        parameters.addAll(updateValues);
        parameters.addAll(whereParameters);
        executeUpdate(createUpdateStatement(), parameters, connection);
    }

    private String createUpdateStatement() {
        return "update " + tableName
            + " set " + join(",", updates(updateFields))
            + (whereParameters.isEmpty() ? "" : " where " + join(" and ", whereConditions));
    }

    private static List<String> updates(List<String> columns) {
        List<String> result = new ArrayList<>();
        for (String column : columns) {
            result.add(column + " = ?");
        }
        return result;
    }

}
