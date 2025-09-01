package org.fluentjdbc;

import java.util.Collection;

/**
 * Collects parameters for insert statements, update statements and where expressions
 */
public class DatabaseQueryParameter {
    private final String whereExpression;
    private final Collection<?> parameters;
    private final String columnName;
    private final String updateExpression;

    public DatabaseQueryParameter(String whereExpression, Collection<?> parameters, String columnName, String updateExpression) {
        this.whereExpression = whereExpression;
        this.parameters = parameters;
        this.columnName = columnName;
        this.updateExpression = updateExpression;
    }

    public DatabaseQueryParameter(String whereExpression, Collection<?> parameters) {
        this.whereExpression = whereExpression;
        this.parameters = parameters;
        this.columnName = null;
        this.updateExpression = null;
    }

    public String getWhereExpression() {
        return whereExpression;
    }

    public Collection<?> getParameters() {
        return parameters;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getUpdateExpression() {
        return updateExpression;
    }
}
