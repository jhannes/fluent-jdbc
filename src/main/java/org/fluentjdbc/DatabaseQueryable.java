package org.fluentjdbc;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    T whereExpressionWithMultipleParameters(String expression, Collection<?> parameters);

    /**
     * Adds the expression to the WHERE-clause and the value to the parameter list. E.g.
     * <code>whereExpression("created_at &gt; ?", earliestDate)</code>
     */
    default T whereExpression(String expression, @Nullable Object parameter) {
        return whereExpressionWithMultipleParameters(expression, Arrays.asList(parameter));
    }

    /**
     * Adds the expression to the WHERE-clause
     */
    default T whereExpression(String expression) {
        return whereExpressionWithMultipleParameters(expression, Arrays.asList());
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    default T whereOptional(String fieldName, @Nullable Object value) {
        if (value == null) return query();
        return where(fieldName, value);
    }

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    default T whereIn(String fieldName, Collection<?> parameters) {
        if (parameters.isEmpty()) {
            return whereExpression(fieldName + " <> " + fieldName);
        }
        return whereExpressionWithMultipleParameters(
                fieldName + " IN (" + parameterString(parameters.size()) + ")",
                parameters
        );
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    default T whereAll(List<String> fields, List<Object> values) {
        for (int i = 0; i < fields.size(); i++) {
            where(fields.get(i), values.get(i));
        }
        return query();
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query
     */
    default T where(String fieldName, @Nullable Object value) {
        return whereExpression(fieldName + " = ?", value);
    }

    /**
     * Returns or creates a query object to be used to add {@link #where(String, Object)} statements and operations
     */
    T query();
}
