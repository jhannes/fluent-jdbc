package org.fluentjdbc;


import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Generate a statement that will result in either an <code>UPDATE ...</code> or <code>INSERT ...</code> depending
 * on whether the row exists already by collecting field names and parameters. Example:
 *
 * <pre>
 * {@link DbContextTable} table = context.table("database_test_table");
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *      int count = table
 *          .where("id", id)
 *          .insertOrUpdate()
 *          .setField("name", "Something")
 *          .setField("code", 102)
 *          .execute(connection);
 * }
 * </pre>
 */
public class DbContextInsertOrUpdateBuilder implements DatabaseUpdatable<DbContextInsertOrUpdateBuilder> {
    private final DbContextTable table;
    private DatabaseInsertOrUpdateBuilder builder;

    public DbContextInsertOrUpdateBuilder(DbContextTable table, DatabaseInsertOrUpdateBuilder builder) {
        this.table = table;
        this.builder = builder;
    }

    /**
     * Adds the fieldName to the SQL statement and the value to the parameter list
     */
    @Override
    public DbContextInsertOrUpdateBuilder setField(String field, @Nullable Object value) {
        return build(builder.setField(field, value));
    }

    /**
     * Adds the fieldName to the SQL statement and the value to the parameter list
     */
    @Override
    public DbContextInsertOrUpdateBuilder setField(String field, String expression, Collection<?> values) {
        return build(builder.setField(field, expression, values));
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DbContextInsertOrUpdateBuilder setFields(List<String> fields, List<?> values) {
        return build(builder.setFields(fields, values));
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DbContextInsertOrUpdateBuilder setFields(Map<String, ?> fields) {
        return build(builder.setFields(fields));
    }

    /**
     * Will execute the UPDATE statement to the database
     */
    public int execute() {
        return builder.execute(table.getConnection());
    }

    private DbContextInsertOrUpdateBuilder build(DatabaseInsertOrUpdateBuilder builder) {
        this.builder = builder;
        return this;
    }
}
