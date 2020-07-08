package org.fluentjdbc;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides a thread context oriented way of manipulating a table. Create from {@link DbContext}.
 * All database operations must be nested in {@link DbContext#startConnection(DataSource)}.
 *
 * <h2>Usage examples</h2>
 * <pre>
 * DbTableContext table = context.table("database_test_table");
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *     table.insert().setField("code", 102).execute();
 *     table.query().where("key", key).orderBy("value").listStrings("value");
 *     table.where("id", id).update().setField("value", newValue).execute();
 *     table.where("id", id).delete();
 *
 *     table.newSaveBuilderWithUUID("id", o.getId()).setField("name", o.getName()).execute();
 * }
 * </pre>
 */
public class DbTableContext implements DatabaseQueryable<DbContextSelectBuilder> {

    private final DatabaseTable table;
    private final DbContext dbContext;

    public DbTableContext(DatabaseTable databaseTable, DbContext dbContext) {
        this.table = databaseTable;
        this.dbContext = dbContext;
    }

    /**
     * Creates a {@link DbContextInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement. Example:
     *
     * <pre>
     *     table.insert().setField("id", id).setField("name", name).execute();
     * </pre>
     *
     */
    public DbContextInsertBuilder insert() {
        return new DbContextInsertBuilder(this);
    }

    /**
     * Creates a {@link #query()} and adds "<code>WHERE fieldName in (?, ?, ?)</code>".
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    @Override
    public DbContextSelectBuilder whereIn(String fieldName, Collection<?> parameters) {
        return query().whereIn(fieldName, parameters);
    }

    /**
     * Creates a {@link #query()} and adds "<code>WHERE fieldName = value</code>" unless value is null
     */
    @Override
    public DbContextSelectBuilder whereOptional(String fieldName, Object value) {
        return query().whereOptional(fieldName, value);
    }

    /**
     * Creates a {@link #query()} and adds the expression to the WHERE-clause
     */
    @Override
    public DbContextSelectBuilder whereExpression(String expression) {
        return query().whereExpression(expression);
    }

    /**
     * Creates a {@link #query()} and adds the expression to the WHERE-clause and
     * the value to the parameter list. E.g. <code>whereExpression("created_at &gt; ?", earliestDate)</code>
     */
    public DbContextSelectBuilder whereExpression(String expression, @Nullable Object value) {
        return query().whereExpression(expression, value);
    }

    /**
     * Creates a {@link #query()} and adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    public DbContextSelectBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters){
        return query().whereExpressionWithMultipleParameters(expression, parameters);
    }

    /**
     * Creates a {@link #query()} and for each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DbContextSelectBuilder whereAll(List<String> fields, List<Object> values) {
        return query().whereAll(fields, values);
    }

    /**
     * Creates a {@link #query()} ready for list-operations without any <code>ORDER BY</code> clause
     */
    public DbContextSelectBuilder unordered() {
        return query();
    }

    /**
     * Creates a {@link #query()} ready for list-operations with <code>ORDER BY ...</code> clause
     */
    public DbContextSelectBuilder orderedBy(String orderByClause) {
        return query().orderBy(orderByClause);
    }

    /**
     * Creates a {@link DbSaveBuilderContext} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database. If idValue is null,
     * {@link DbSaveBuilderContext} will attempt to use the table's autogeneration of primary keys
     * if there is no row with matching unique keys
     */
    public DbSaveBuilderContext<Long> newSaveBuilder(String idColumn, Long idValue) {
        return save(table.newSaveBuilder(idColumn, idValue));
    }

    /**
     * Creates a {@link DbSaveBuilderContext} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database. Throws exception if idValue is null
     */
    public DbSaveBuilderContext<String> newSaveBuilderWithString(String idColumn, String idValue) {
        return save(table.newSaveBuilderWithString(idColumn, idValue));
    }

    /**
     * Creates a {@link DbSaveBuilderContext} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database.
     * Generates UUID.randomUUID if idValue is null and row with matching unique keys does not already exist
     */
    public DbSaveBuilderContext<UUID> newSaveBuilderWithUUID(String field, UUID uuid) {
        return save(table.newSaveBuilderWithUUID(field, uuid));
    }

    /**
     * Associates the argument {@link DatabaseSaveResult} with this {@link DbTableContext}'s {@link DbContext}
     */
    public <T> DbSaveBuilderContext<T> save(DatabaseSaveBuilder<T> saveBuilder) {
        return new DbSaveBuilderContext<>(this, saveBuilder);
    }

    /**
     * Retrieves the underlying or cached value of the retriever argument. This cache is per
     * {@link DbContextConnection} and is evicted when the connection is closed. The key is in context
     * of this table, so different tables can have the same key without collision
     */
    public <KEY,ENTITY> Optional<ENTITY> cache(KEY key, RetrieveMethod<KEY, ENTITY> retriever) {
        return dbContext.cache(getTable().getTableName(), key, retriever);
    }

    /**
     * Create a {@link DbTableAliasContext} associated with this {@link DbTableContext} which can
     * be used to <code>JOIN</code> statements with {@link DbTableAliasContext#join(DatabaseColumnReference, DatabaseColumnReference)}.
     *
     * @see DbJoinedSelectContext
     */
    public DbTableAliasContext alias(String alias) {
        return new DbTableAliasContext(this, alias);
    }

    /**
     * Creates a {@link DbContextSelectBuilder} which allows you to query the table by
     * generating <code>"SELECT ..."</code> statements
     */
    @Override
    public DbContextSelectBuilder query() {
        return new DbContextSelectBuilder(this);
    }

    public <T> DbSyncBuilderContext<T> sync(List<T> entities) {
        return new DbSyncBuilderContext<>(this, entities);
    }

    /**
     * Creates a {@link DbBulkInsertContext} object to fluently generate a <code>INSERT ...</code> statement
     * for a list of objects. Example:
     *
     * <pre>
     *     public void saveAll(List&lt;TagType&gt; tagTypes) {
     *         tagTypesTable.bulkInsert(tagTypes)
     *             .setField("name", TagType::getName)
     *             .generatePrimaryKeys("id", TagType::setId)
     *             .execute();
     *     }
     * </pre>
     */
    public <T> DbBulkInsertContext<T> bulkInsert(Stream<T> objects) {
        return bulkInsert(objects.collect(Collectors.toList()));
    }

    /**
     * Creates a {@link DbBulkInsertContext} object to fluently generate a <code>INSERT ...</code> statement
     * for a list of objects. Example:
     *
     * <pre>
     *     public void saveAll(List&lt;TagType&gt; tagTypes) {
     *         tagTypesTable.bulkInsert(tagTypes)
     *             .setField("name", TagType::getName)
     *             .generatePrimaryKeys("id", TagType::setId)
     *             .execute();
     *     }
     * </pre>
     */
    public <T> DbBulkInsertContext<T> bulkInsert(Iterable<T> objects) {
        return new DbBulkInsertContext<>(this, table.bulkInsert(objects));
    }

    public <T> DbBulkDeleteContext<T> bulkDelete(Stream<T> objects) {
        return bulkDelete(objects.collect(Collectors.toList()));
    }

    public <T> DbBulkDeleteContext<T> bulkDelete(Iterable<T> objects) {
        return new DbBulkDeleteContext<>(this, table.bulkDelete(objects));
    }

    public <T> DbBulkUpdateContext<T> bulkUpdate(Stream<T> objects) {
        return bulkUpdate(objects.collect(Collectors.toList()));
    }

    private <T> DbBulkUpdateContext<T> bulkUpdate(List<T> objects) {
        return new DbBulkUpdateContext<>(this, table.bulkUpdate(objects));
    }

    public DatabaseTable getTable() {
        return table;
    }

    public DbContext getDbContext() {
        return dbContext;
    }

    public Connection getConnection() {
        return dbContext.getThreadConnection();
    }
}
