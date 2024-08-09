package org.fluentjdbc;

import java.util.Collection;

public class MultipleRowsReturnedException extends IllegalArgumentException {
    private final String statement;
    private final Collection<?> parameters;

    public MultipleRowsReturnedException(String statement, Collection<?> parameters) {
        super("statement " + statement + " returned multiple rows for " + parameters);
        this.statement = statement;
        this.parameters = parameters;
    }

    public String getStatement() {
        return statement;
    }

    public Collection<?> getParameters() {
        return parameters;
    }
}
