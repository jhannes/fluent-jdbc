package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Used to build the <code>WHERE ...</code> clause of SQL statements such as SELECT, UPDATE and DELETE.
 * Add clauses with {@link #where(String, Object)}, {@link #whereIn(String, Collection)} etc. Then
 * get the string for the WHERE clause with {@link #whereClause()} and the parameters with {@link #getParameters()}
 */
@ParametersAreNonnullByDefault
public class DatabaseWhereBuilder implements DatabaseQueryable<DatabaseWhereBuilder> {

    private final List<String> columns = new ArrayList<>();
    private final List<String> columnExpressions = new ArrayList<>();
    private final List<String> conditions = new ArrayList<>();
    private final List<Object> parameters = new ArrayList<>();

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpressionWithParameterList("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    @Override
    public DatabaseWhereBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        this.conditions.add("(" + expression + ")");
        this.parameters.addAll(parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereColumnValues("json_column", "?::json", jsonString)</code>
     */
    @Override
    public DatabaseWhereBuilder whereColumnValuesEqual(String column, String expression, Collection<?> parameters) {
        //noinspection ResultOfMethodCallIgnored
        whereExpressionWithParameterList(column + " = " + expression, parameters);
        columns.add(column);
        columnExpressions.add(expression);
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
        return conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    }

    /**
     * Returns all parameters added with <code>.whereXXX()</code> method calls
     */
    @CheckReturnValue
    public List<Object> getParameters() {
        return parameters;
    }

    public List<String> getColumns() {
        return columns;
    }
}
