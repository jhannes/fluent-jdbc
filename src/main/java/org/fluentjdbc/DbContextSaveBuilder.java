package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import javax.annotation.CheckReturnValue;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Generate <code>INSERT</code> or <code>UPDATE</code> statements on {@link DbContextTable} by
 * collecting field names and parameters. Support autogeneration of primary keys and unique natural
 * keys. Example:
 *
 * <pre>
 * {@link DbContextTable} table = context.table("database_test_table");
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *     DatabaseSaveResult&lt;Long&gt; result = table.newSaveBuilder("id", object.getId())
 *         .uniqueKey("name", object.getName())
 *         .setField("value", object.getValue())
 *         .execute();
 *     object.setId(result.getId());
 * }
 * </pre>
 *
 * @see DatabaseInsertBuilder
 */
public class DbContextSaveBuilder<T> {

    private final DbContextTable table;
    private final DatabaseSaveBuilder<T> saveBuilder;

    public DbContextSaveBuilder(DbContextTable table, DatabaseSaveBuilder<T> saveBuilder) {
        this.table = table;
        this.saveBuilder = saveBuilder;
    }

    /**
     * Specify a natural key for this table. If the <code>id</code> is null and there is a unique key
     * match, UPDATE is called with this row and the existing primary key is returned, instead of INSERT.
     * If more than one uniqueKey, fluent-jdbc assumes a composite unique constraint, that is <em>all</em>
     * fields must match
     */
    @CheckReturnValue
    public DbContextSaveBuilder<T> uniqueKey(String fieldName, Object fieldValue) {
        //noinspection ResultOfMethodCallIgnored
        saveBuilder.uniqueKey(fieldName, fieldValue);
        return this;
    }

    /**
     * Specify a column name to be saved
     */
    @CheckReturnValue
    public DbContextSaveBuilder<T> setField(String fieldName, Object fieldValue) {
        //noinspection ResultOfMethodCallIgnored
        saveBuilder.setField(fieldName, fieldValue);
        return this;
    }

    /**
     * Executes the <code>UPDATE</code> or <code>INSERT</code> statement and returns a
     * {@link DatabaseSaveResult} which explains what operation was executed.
     * See {@link DatabaseSaveBuilder#execute(Connection)}
     */
    public DatabaseSaveResult<T> execute() {
        try {
            return saveBuilder.execute(table.getConnection());
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

}
