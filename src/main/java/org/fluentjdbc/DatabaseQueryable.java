package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.fluentjdbc.DatabaseStatement.parameterString;

/**
 * Interface to build <code>WHERE</code>-expressions for <code>SELECT</code>-statements. Usage:
 *
 * <pre>
 * table
 *     .where("firstName", firstName)
 *     .whereExpression("lastName like ?", "joh%")
 *     .whereIn("status", statuses)
 *     .orderBy("lastName")
 *     .list(...)
 * </pre>
 */
public interface DatabaseQueryable<T extends DatabaseQueryable<T>> {

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpressionWithParameterList("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    @CheckReturnValue
    default T whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        return query().whereExpressionWithParameterList(expression, parameters);
    }

    /**
     * Adds the expression to the WHERE-clause and the value to the parameter list. E.g.
     * <code>whereExpression("created_at &gt; ?", earliestDate)</code>
     */
    @CheckReturnValue
    default T whereExpression(String expression, @Nullable Object parameter) {
        return whereExpressionWithParameterList(expression, Collections.singletonList(parameter));
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereColumnValues("json_column", "?::json", jsonString)</code>
     */
    @CheckReturnValue
    default T whereColumnValuesEqual(String column, String expression, Collection<?> parameters) {
        return query().whereColumnValuesEqual(column, expression, parameters);
    }

    /**
     * Adds the expression to the WHERE-clause
     */
    @CheckReturnValue
    default T whereExpression(String expression) {
        return whereExpressionWithParameterList(expression, Collections.emptyList());
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @CheckReturnValue
    default T whereOptional(String fieldName, @Nullable Object value) {
        if (value == null) return query();
        return where(fieldName, value);
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @CheckReturnValue
    default T whereOptional(DatabaseColumnReference column, @Nullable Object value) {
        if (value == null) return query();
        return where(column, value);
    }

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    @CheckReturnValue
    default T whereIn(String fieldName, Collection<?> parameters) {
        if (parameters.isEmpty()) {
            return whereExpression(fieldName + " <> " + fieldName);
        }
        return whereExpressionWithParameterList(
                fieldName + " IN (" + parameterString(parameters.size()) + ")",
                parameters
        );
    }

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    @CheckReturnValue
    default T whereIn(DatabaseColumnReference fieldName, Collection<?> parameters) {
        return whereIn(fieldName.getQualifiedColumnName(), parameters);
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    @CheckReturnValue
    default T whereAll(List<String> fields, List<Object> values) {
        T query = query();
        for (int i = 0; i < fields.size(); i++) {
            query = query.where(fields.get(i), values.get(i));
        }
        return query;
    }

    /**
     * For each key and value adds "<code>WHERE fieldName = value</code>" to the query
     */
    @CheckReturnValue
    default T whereAll(Map<String, ?> fields) {
        T query = query();
        for (Map.Entry<String, ?> entry : fields.entrySet()) {
            query = query.where(entry.getKey(), entry.getValue());
        }
        return query;
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query
     */
    @CheckReturnValue
    default T where(String fieldName, @Nullable Object value) {
        return whereColumnValuesEqual(fieldName, "?", Collections.singletonList(value));
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query
     */
    @CheckReturnValue
    default T where(DatabaseColumnReference column, @Nullable Object value) {
        return whereColumnValuesEqual(column.getQualifiedColumnName(), "?", Collections.singletonList(value));
    }

    /**
     * Returns or creates a query object to be used to add {@link #where(String, Object)} statements and operations
     */
    @CheckReturnValue
    T query();
}
