package org.fluentjdbc;

import java.sql.PreparedStatement;
import java.util.function.Function;

/**
 * Fluently generate a <code>DELETE ... WHERE ...</code> statement for a list of objects.
 * Create with a list of object and use {@link #where(String, Function)} to add a function
 * which will be called for each object to create the <code>WHERE field = ?</code> value.
 *
 * <p>Example:</p>
 *
 * <pre>
 *     public void deleteAll(List&lt;TagType&gt; tagTypes) {
 *         tagTypesTable.bulkDelete(tagTypes)
 *              .where("id", TagType::getId)
 *              .execute(connection);
 *     }
 * </pre>
 */
public class DbBulkDeleteContext<T> implements DatabaseBulkQueryable<T, DbBulkDeleteContext<T>> {
    private final DbTableContext tableContext;
    private final DatabaseBulkDeleteBuilder<T> builder;

    public DbBulkDeleteContext(DbTableContext tableContext, DatabaseBulkDeleteBuilder<T> builder) {
        this.tableContext = tableContext;
        this.builder = builder;
    }

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk update
     * to extract the values for the <code>WHERE fieldName = ?</code> clause
     */
    @Override
    public DbBulkDeleteContext<T> where(String field, Function<T, ?> value) {
        builder.where(field, value);
        return this;
    }

    /**
     * Executes <code>DELETE FROM table WHERE field = ? AND ...</code>
     * and calls {@link PreparedStatement#addBatch()} for each row
     *
     * @return the sum count of all the rows deleted
     */
    public int execute() {
        return builder.execute(tableContext.getConnection());
    }
}
