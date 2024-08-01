package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Thrown on {@link DatabaseStatement#singleObject} when no rows were returned
 */
public class NoRowsReturnedException extends RuntimeException {
    @Nonnull
    private final String statement;
    @Nonnull
    private final Collection<Object> parameters;

    public NoRowsReturnedException(String statement, Collection<Object> parameters) {
        super("statement " + statement + " returned no rows for " + parameters);
        this.statement = statement;
        this.parameters = parameters;
    }

    public String getStatement() {
        return statement;
    }

    public Collection<Object> getParameters() {
        return parameters;
    }
}
