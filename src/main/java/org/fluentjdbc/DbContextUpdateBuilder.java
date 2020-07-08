package org.fluentjdbc;

import java.util.Collection;

/**
 * Generate <code>UPDATE</code> insert statements by collecting field names and parameters. Support
 * autogeneration of primary keys. Example:
 *
 * <pre>
 * DbTableContext table = context.table("database_test_table");
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

    private final DbTableContext tableContext;
    private final DatabaseUpdateBuilder updateBuilder;

    public DbContextUpdateBuilder(DbTableContext tableContext, DatabaseUpdateBuilder updateBuilder) {
        this.tableContext = tableContext;
        this.updateBuilder = updateBuilder;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DbContextUpdateBuilder setFields(Collection<String> fields, Collection<?> values) {
        updateBuilder.setFields(fields, values);
        return this;
    }

    /**
     * Adds the fieldName to the SQL statement and the value to the parameter list
     */
    @Override
    public DbContextUpdateBuilder setField(String field, Object value) {
        updateBuilder.setField(field, value);
        return this;
    }

    /**
     * Will execute the UPDATE statement to the database
     */
    public int execute() {
        return updateBuilder.execute(tableContext.getConnection());
    }


}
