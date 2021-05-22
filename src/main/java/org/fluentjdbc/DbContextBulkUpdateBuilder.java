package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.util.function.Function;

/**
 * Fluently generate a <code>UPDATE ...</code> statement for a list of objects. Create with a list of object
 * and use {@link #setField(String, Function)} to pass in a function that will be called for each object
 * in the list to create the <code>SET field = ?</code> value and {@link #where(String, Function)}
 * which will be called for each object to create the <code>WHERE field = ?</code> value.
 *
 * <p>Example:</p>
 *
 * <pre>
 *     public void saveAll(List&lt;TagType&gt; tagTypes) {
 *         tagTypesTable.bulkUpdate(tagTypes)
 *              .where("id", TagType::getId)
 *              .setField("name", TagType::getName)
 *              .execute();
 *     }
 * </pre>
 */
public class DbContextBulkUpdateBuilder<T> implements
        DatabaseBulkQueryable<T, DbContextBulkUpdateBuilder<T>>, DatabaseBulkUpdatable<T, DbContextBulkUpdateBuilder<T>> {

    private final DbContextTable table;
    private final DatabaseBulkUpdateBuilder<T> builder;

    public DbContextBulkUpdateBuilder(@Nonnull DbContextTable table, DatabaseBulkUpdateBuilder<T> builder) {
        this.table = table;
        this.builder = builder;
    }

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk update
     * to extract the values for the <code>WHERE fieldName = ?</code> clause
     */
    @Override
    public DbContextBulkUpdateBuilder<T> where(String field, Function<T, ?> value) {
        //noinspection ResultOfMethodCallIgnored
        builder.where(field, value);
        return this;
    }

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk update
     * to extract the values for the <code>SET fieldName = ?</code> clause
     */
    @Override
    public DbContextBulkUpdateBuilder<T> setField(String fieldName, Function<T, Object> transformer) {
        //noinspection ResultOfMethodCallIgnored
        builder.setField(fieldName, transformer);
        return this;
    }

    /**
     * Executes <code>UPDATE table SET field = ?, ... WHERE field = ? AND ...</code>
     * and calls {@link PreparedStatement#addBatch()} for each row
     *
     * @return the count of rows that were updated
     */
    public int execute() {
        return builder.execute(table.getConnection());
    }

}
