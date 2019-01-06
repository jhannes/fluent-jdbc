package org.fluentjdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

class DatabaseDeleteBuilder extends DatabaseStatement {

    private DatabaseTable table;

    private final List<String> whereConditions = new ArrayList<>();
    private final List<Object> whereParameters = new ArrayList<>();

    public DatabaseDeleteBuilder(DatabaseTable table) {
        this.table = table;
    }

    DatabaseDeleteBuilder setWhereFields(List<String> whereConditions, List<Object> whereParameters) {
        this.whereConditions.addAll(whereConditions);
        this.whereParameters.addAll(whereParameters);
        return this;
    }

    public void execute(Connection connection) {
        executeUpdate(createDeleteStatement(), whereParameters, connection);
    }

    private String createDeleteStatement() {
        return "delete from " + table.getTableName() + " where "  + join(" and ", whereConditions);
    }
}
