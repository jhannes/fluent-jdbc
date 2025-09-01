package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>Provides a starting point for for fluent-jdbc with explicit Connection management.</p>
 *
 * <p>Example</p>
 * <pre>
 * DatabaseTable table = new DatabaseTableImpl("database_table_test_table");
 * Object id = table.insert()
 *     .setPrimaryKey("id", null)
 *     .setField("code", 1002)
 *     .setField("name", "insertTest")
 *     .execute(connection);
 *
 * List&lt;Long&gt; result = table.where("name", "insertTest").orderBy("code").listLongs(connection, "code");
 * table.where("id", id).setField("name", updatedName).execute(connection);
 * table.where("id", id).delete(connection);
 * </pre>
 */
@ParametersAreNonnullByDefault
public class DatabaseTableImpl implements DatabaseTable {

    private final String tableName;
    private final DatabaseStatementFactory factory;

    public DatabaseTableImpl(String tableName) {
        this(tableName, new DatabaseStatementFactory(DatabaseReporter.LOGGING_REPORTER));
    }

    public DatabaseTableImpl(String tableName, DatabaseStatementFactory factory) {
        this.tableName = tableName;
        this.factory = factory;
    }

    @Override
    public DatabaseTableQueryBuilder where(DatabaseQueryParameter parameter) {
        return query().where(parameter);
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @Override
    @CheckReturnValue
    public DatabaseTableQueryBuilder unordered() {
        return new DatabaseTableQueryBuilder(this);
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    @Override
    @CheckReturnValue
    public DatabaseTableQueryBuilder orderBy(String orderByClause) {
        return query().orderBy(orderByClause);
    }

    @Override
    @CheckReturnValue
    public DatabaseTableAlias alias(String alias) {
        return new DatabaseTableAlias(this, alias);
    }

    @Override
    @CheckReturnValue
    public String getTableName() {
        return tableName;
    }

    /**
     * Creates a {@link DatabaseSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database. If idValue is null,
     * {@link DatabaseSaveBuilder} will attempt to use the table's autogeneration of primary keys
     * if there is no row with matching unique keys
     */
    @Override
    @CheckReturnValue
    public DatabaseSaveBuilder<Long> newSaveBuilder(String idField, @Nullable Long id) {
        return new DatabaseSaveBuilderWithLong(this, idField, id);
    }

    /**
     * Creates a {@link DatabaseSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database. Throws exception if idValue is null
     */
    @Override
    @CheckReturnValue
    public DatabaseSaveBuilder<String> newSaveBuilderWithString(String idField, String id) {
        return new DatabaseSaveBuilderWithoutGeneratedKeys<>(this, idField, id);
    }

    /**
     * Use instead of {@link #newSaveBuilder} if the database driver does not
     * support RETURN_GENERATED_KEYS
     */
    @Override
    @CheckReturnValue
    public DatabaseSaveBuilder<Long> newSaveBuilderNoGeneratedKeys(String idField, @Nullable Long id) {
        return new DatabaseSaveBuilderWithoutGeneratedKeys<>(this, idField, id);
    }

    /**
     * Creates a {@link DatabaseSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database.
     * Generates UUID.randomUUID if idValue is null and row with matching unique keys does not already exist
     */
    @Override
    @CheckReturnValue
    public DatabaseSaveBuilderWithUUID newSaveBuilderWithUUID(String idField, @Nullable UUID id) {
        return new DatabaseSaveBuilderWithUUID(this, idField, id);
    }

    /**
     * Creates a query object to be used to add {@link #where(String, Object)} statements and operations
     */
    @Override
    @CheckReturnValue
    public DatabaseTableQueryBuilder query() {
        return new DatabaseTableQueryBuilder(this);
    }

    /**
     * Creates a {@link DatabaseInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement
     */
    @Override
    @CheckReturnValue
    public DatabaseInsertBuilder insert() {
        return new DatabaseInsertBuilder(this);
    }

    /**
     * Creates a {@link DatabaseBulkInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement
     * for a list of objects. Example:
     *
     * <pre>
     *     public void saveAll(List&lt;TagType&gt; tagTypes, Connection connection) {
     *         tagTypesTable.bulkInsert(tagTypes)
     *             .setField("name", TagType::getName)
     *             .generatePrimaryKeys("id", TagType::setId)
     *             .execute(connection);
     *     }
     * </pre>
     */
    @Override
    @CheckReturnValue
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(Iterable<T> objects) {
        return new DatabaseBulkInsertBuilder<>(this, objects);
    }

    /**
     * Creates a {@link DatabaseBulkInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement
     * for a list of objects. Example:
     *
     * <pre>
     *     public void saveAll(Stream&lt;TagType&gt; tagTypes, Connection connection) {
     *         tagTypesTable.bulkInsert(tagTypes)
     *             .setField("name", TagType::getName)
     *             .generatePrimaryKeys("id", TagType::setId)
     *             .execute(connection);
     *     }
     * </pre>
     */
    @Override
    @CheckReturnValue
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(Stream<T> objects) {
        return bulkInsert(objects.collect(Collectors.toList()));
    }

    /**
     * Creates a {@link DatabaseBulkDeleteBuilder} object to fluently generate a <code>DELETE ...</code> statement
     * for a list of objects. Example:
     *
     * <p>Example:</p>
     *
     * <pre>
     *     public void deleteAll(List&lt;TagType&gt; tagTypes, Connection connection) {
     *         tagTypesTable.bulkDelete(tagTypes)
     *              .where("id", TagType::getId)
     *              .execute(connection);
     *     }
     * </pre>
     */
    @Override
    @CheckReturnValue
    public <T> DatabaseBulkDeleteBuilder<T> bulkDelete(Iterable<T> objects) {
        return new DatabaseBulkDeleteBuilder<>(this, objects);
    }

    @Override
    @CheckReturnValue
    public <T> DatabaseBulkUpdateBuilder<T> bulkUpdate(Iterable<T> objects) {
        return new DatabaseBulkUpdateBuilder<>(this, objects);
    }

    /**
     * Creates a {@link DatabaseUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    @Override
    @CheckReturnValue
    public DatabaseUpdateBuilder update() {
        return new DatabaseUpdateBuilder(this);
    }

    /**
     * Creates a {@link DatabaseInsertOrUpdateBuilder} object to fluently generate a statement that will result
     * in either an <code>UPDATE ...</code> or <code>INSERT ...</code> depending on whether the row exists already
     */
    @Override
    public DatabaseInsertOrUpdateBuilder insertOrUpdate() {
        return new DatabaseInsertOrUpdateBuilder(this);
    }

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code>
     */
    @Override
    @CheckReturnValue
    public DatabaseDeleteBuilder delete() {
        return new DatabaseDeleteBuilder(this);
    }

    @Override
    public DatabaseStatement newStatement(String operation, String sql, Collection<?> parameters) {
        return factory.newStatement(tableName, operation, sql, parameters);
    }
}
