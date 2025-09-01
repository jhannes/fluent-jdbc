package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import java.util.List;
import java.util.Map;

/**
 * Generate <code>INSERT</code> statements by collecting field names and parameters. Support
 * autogeneration of primary keys. Example:
 *
 * <pre>
 * {@link DbContextTable} table = context.table("database_test_table");
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *     Long id = table.insert()
 *      .setPrimaryKey("id", (Long)null)
 *      .setField("name", "Something")
 *      .setField("code", 102)
 *      .execute();
 * }
 * </pre>
 *
 * @see DatabaseInsertBuilder
 */
public class DbContextInsertBuilder implements DatabaseUpdatable<DbContextInsertBuilder> {

    public class DbContextInsertBuilderWithPk<T> {

        private final DatabaseInsertWithPkBuilder<T> builder;

        public DbContextInsertBuilderWithPk(DatabaseInsertWithPkBuilder<T> builder) {
            this.builder = builder;
        }

        @CheckReturnValue
        public DbContextInsertBuilderWithPk<T> setField(String fieldName, Object parameter) {
            //noinspection ResultOfMethodCallIgnored
            builder.setField(fieldName, parameter);
            return this;
        }

        public T execute() {
            return builder.execute(dbContextTable.getConnection());
        }
    }

    private DatabaseInsertBuilder builder;
    private final DbContextTable dbContextTable;

    public DbContextInsertBuilder(DbContextTable dbContextTable) {
        this.dbContextTable = dbContextTable;
        builder = dbContextTable.getTable().insert();
    }

    /**
     * Adds primary key to the <code>INSERT</code> statement if idValue is not null. If idValue is null
     * this will {@link java.sql.PreparedStatement#execute(String, String[])} to generate the primary
     * key using the underlying table autogeneration mechanism
     */
    @CheckReturnValue
    public <T> DbContextInsertBuilderWithPk<T> setPrimaryKey(String idField, T idValue) {
        DatabaseInsertWithPkBuilder<T> setPrimaryKey = builder.setPrimaryKey(idField, idValue);
        return new DbContextInsertBuilderWithPk<>(setPrimaryKey);
    }

    /**
     * Adds parameters to the <code>INSERT (fieldName) VALUES (expression)</code> and to the list of parameters
     */
    public DbContextInsertBuilder addParameters(List<DatabaseQueryParameter> queryParameters) {
        return build(builder.addParameters(queryParameters));
    }

    /**
     * Adds a parameter to the <code>INSERT (fieldName) VALUES (expression)</code> and to the list of parameters
     */
    @CheckReturnValue
    public DbContextInsertBuilder addParameter(DatabaseQueryParameter parameter) {
        return build(builder.addParameter(parameter));
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DbContextInsertBuilder setFields(List<String> fields, List<?> values) {
        return build(builder.setFields(fields, values));
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DbContextInsertBuilder setFields(Map<String, ?> fields) {
        return build(builder.setFields(fields));
    }

    /**
     * Executes the insert statement and returns the number of rows inserted
     */
    public int execute() {
        return builder.execute(dbContextTable.getConnection());
    }

    private DbContextInsertBuilder build(DatabaseInsertBuilder builder) {
        this.builder = builder;
        return this;
    }
}
