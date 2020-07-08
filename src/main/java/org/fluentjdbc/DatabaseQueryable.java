package org.fluentjdbc;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Interface to build <code>WHERE</code>-expressions for <code>SELECT</code>-statements. Usage:
 *
 * <pre>
 * table
 *  .where("firstName", firstName)
 *  .whereExpression("lastName like ?", "joh%")
 *  .whereIn("status", statuses)
 *  .orderBy("lastName")
 *  .list(...)
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
    T whereExpression(String expression, @Nullable Object value);

    /**
     * Adds the expression to the WHERE-clause
     */
    T whereExpression(String expression);

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    T whereOptional(String fieldName, @Nullable Object value);

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    T whereIn(String fieldName, Collection<?> parameters);

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    T whereAll(List<String> fields, List<Object> values);

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query
     */
    default T where(String fieldName, @Nullable Object value) {
        return whereExpression(fieldName + " = ?", value);
    }

    T query();
}
