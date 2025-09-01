package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to build the <code>WHERE ...</code> clause of SQL statements such as SELECT, UPDATE and DELETE.
 * Add clauses with {@link #where(String, Object)}, {@link #whereIn(String, Collection)} etc. Then
 * get the string for the WHERE clause with {@link #whereClause()} and the parameters with {@link #getParameters()}
 */
@ParametersAreNonnullByDefault
public class DatabaseWhereBuilder implements DatabaseQueryable<DatabaseWhereBuilder> {

    private final List<DatabaseQueryParameter> queryParameters = new ArrayList<>();

    /**
     * Adds the parameter to the WHERE-clause and all the parameter list.
     * E.g. <code>where(new DatabaseQueryParameter("created_at between ? and ?", List.of(earliestDate, latestDate)))</code>
     */
    @Override
    public DatabaseWhereBuilder where(DatabaseQueryParameter parameter) {
        this.queryParameters.add(parameter);
        return this;
    }

    /**
     * Implemented as <code>return this</code> for compatibility purposes
     */
    @Override
    public DatabaseWhereBuilder query() {
        return this;
    }

    /**
     * If any conditions were added, returns <code>WHERE condition1 AND condition2 AND condition2 ...</code>.
     * If no conditions were added, returns the empty string
     */
    @CheckReturnValue
    public String whereClause() {
        return queryParameters.isEmpty()
                ? ""
                : " WHERE " + queryParameters.stream().map(DatabaseQueryParameter::getWhereExpression).collect(Collectors.joining(" AND "));
    }

    /**
     * Returns all parameters added with <code>.whereXXX()</code> method calls
     */
    @CheckReturnValue
    public List<Object> getParameters() {
        return queryParameters.stream().flatMap(p -> p.getParameters().stream()).collect(Collectors.toList());
    }

    List<DatabaseQueryParameter> getQueryParameters() {
        return queryParameters;
    }
}
