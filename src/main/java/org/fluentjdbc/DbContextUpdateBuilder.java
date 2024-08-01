package org.fluentjdbc;

import java.util.List;
import java.util.Map;

/**
 * Generate <code>UPDATE</code> insert statements by collecting field names and parameters. Support
 * autogeneration of primary keys. Example:
 *
 * <pre>
 * {@link DbContextTable} table = context.table("database_test_table");
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *      int count = table
 *          .where("id", id)
 *          .update()
 *          .setField("name", "Something")
 *          .setField("code", 102)
 *          .execute(connection);
 * }
 * </pre>
 */
public class DbContextUpdateBuilder implements DatabaseUpdatable<DbContextUpdateBuilder> {

    private final DbContextTable table;
    private DatabaseUpdateBuilder builder;

    public DbContextUpdateBuilder(DbContextTable table, DatabaseUpdateBuilder builder) {
        this.table = table;
        this.builder = builder;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DbContextUpdateBuilder setFields(List<String> fields, List<?> values) {
        return build(builder.setFields(fields, values));
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DbContextUpdateBuilder setFields(Map<String, ?> fields) {
        return build(builder.setFields(fields));
    }

    /**
     * Adds the fieldName to the SQL statement and the value to the parameter list
     */
    @Override
    public DbContextUpdateBuilder setField(String field, Object value) {
        return build(builder.setField(field, value));
    }

    /**
     * Will execute the UPDATE statement to the database
     */
    public int execute() {
        return builder.execute(table.getConnection());
    }

    private DbContextUpdateBuilder build(DatabaseUpdateBuilder builder) {
        this.builder = builder;
        return this;
    }
}
