package org.fluentjdbc;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides a thread context oriented way of manipulating a table. Create from {@link DbContext}.
 * All database operations must be nested in {@link DbContext#startConnection(DataSource)}.
 * 
 *<h2>Usage examples</h2>
 * <pre>
 * {@link DbContextTable} table = context.table("database_test_table");
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *     table.{@link #insert()}.setField("code", 102).execute();
 *     table.{@link #query()}.where("key", key).orderBy("value").listStrings("value");
 *     table.where("id", id).update().setField("value", newValue).execute();
 *     table.where("id", id).delete();
 *
 *     table.newSaveBuilderWithUUID("id", o.getId()).setField("name", o.getName()).execute();
 * }
 * </pre>
 */
public class DbContextTable implements DatabaseQueryable<DbContextSelectBuilder> {

    private final DatabaseTable table;
    private final DbContext dbContext;

    public DbContextTable(DatabaseTable databaseTable, DbContext dbContext) {
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
     * Creates a {@link DbContextSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database. If idValue is null,
     * {@link DbContextSaveBuilder} will attempt to use the table's autogeneration of primary keys
     * if there is no row with matching unique keys
     */
    public DbContextSaveBuilder<Long> newSaveBuilder(String idColumn, Long idValue) {
        return save(table.newSaveBuilder(idColumn, idValue));
    }

    /**
     * Creates a {@link DbContextSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database. Throws exception if idValue is null
     */
    public DbContextSaveBuilder<String> newSaveBuilderWithString(String idColumn, String idValue) {
        return save(table.newSaveBuilderWithString(idColumn, idValue));
    }

    /**
     * Creates a {@link DbContextSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database.
     * Generates UUID.randomUUID if idValue is null and row with matching unique keys does not already exist
     */
    public DbContextSaveBuilder<UUID> newSaveBuilderWithUUID(String field, UUID uuid) {
        return save(table.newSaveBuilderWithUUID(field, uuid));
    }

    /**
     * Associates the argument {@link DatabaseSaveResult} with this {@link DbContextTable}'s {@link DbContext}
     */
    public <T> DbContextSaveBuilder<T> save(DatabaseSaveBuilder<T> saveBuilder) {
        return new DbContextSaveBuilder<>(this, saveBuilder);
    }

    /**
     * Retrieves the underlying or cached value of the retriever argument. This cache is per
     * {@link DbContextConnection} and is evicted when the connection is closed. The key is in context
     * of this table, so different tables can have the same key without collision
     */
    public <KEY,ENTITY> Optional<ENTITY> cache(KEY key, DbContext.RetrieveMethod<KEY, ENTITY> retriever) {
        return dbContext.cache(getTable().getTableName(), key, retriever);
    }

    /**
     * Create a {@link DbContextTableAlias} associated with this {@link DbContextTable} which can
     * be used to <code>JOIN</code> statements with {@link DbContextTableAlias#join(DatabaseColumnReference, DatabaseColumnReference)}.
     *
     * @see DbContextJoinedSelectBuilder
     */
    public DbContextTableAlias alias(String alias) {
        return new DbContextTableAlias(this, alias);
    }

    /**
     * Creates a {@link DbContextSelectBuilder} which allows you to query the table by
     * generating <code>"SELECT ..."</code> statements
     */
    @Override
    public DbContextSelectBuilder query() {
        return new DbContextSelectBuilder(this);
    }

    public <T> DbContextSyncBuilder<T> sync(List<T> entities) {
        return new DbContextSyncBuilder<>(this, entities);
    }

    /**
     * Creates a {@link DbContextBuildInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement
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
    public <T> DbContextBuildInsertBuilder<T> bulkInsert(Stream<T> objects) {
        return bulkInsert(objects.collect(Collectors.toList()));
    }

    /**
     * Creates a {@link DbContextBuildInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement
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
    public <T> DbContextBuildInsertBuilder<T> bulkInsert(Iterable<T> objects) {
        return new DbContextBuildInsertBuilder<>(this, table.bulkInsert(objects));
    }

    /**
     * Creates a {@link DbContextBulkDeleteBuilder} object to fluently generate a <code>DELETE ...</code> statement
     * for a list of objects. Example:
     *
     * <p>Example:</p>
     *
     * <pre>
     *     public void deleteAll(Stream&lt;TagType&gt; tagTypes) {
     *         tagTypesTable.bulkDelete(tagTypes)
     *              .where("id", TagType::getId)
     *              .execute();
     *     }
     * </pre>
     */
    public <T> DbContextBulkDeleteBuilder<T> bulkDelete(Stream<T> objects) {
        return bulkDelete(objects.collect(Collectors.toList()));
    }

    /**
     * Creates a {@link DbContextBulkDeleteBuilder} object to fluently generate a <code>DELETE ...</code> statement
     * for a list of objects. Example:
     *
     * <p>Example:</p>
     *
     * <pre>
     *     public void deleteAll(List&lt;TagType&gt; tagTypes) {
     *         tagTypesTable.bulkDelete(tagTypes)
     *              .where("id", TagType::getId)
     *              .execute();
     *     }
     * </pre>
     */
    public <T> DbContextBulkDeleteBuilder<T> bulkDelete(Iterable<T> objects) {
        return new DbContextBulkDeleteBuilder<>(this, table.bulkDelete(objects));
    }

    /**
     * Creates a {@link DbContextBulkUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     * for a list of objects. Example:
     *
     * <pre>
     *     public void updateAll(Stream&lt;TagType&gt; tagTypes) {
     *         tagTypesTable.bulkUpdate(tagTypes)
     *              .where("id", TagType::getId)
     *              .setField("name", TagType::getName)
     *              .execute();
     *     }
     * </pre>
     */
    public <T> DbContextBulkUpdateBuilder<T> bulkUpdate(Stream<T> objects) {
        return bulkUpdate(objects.collect(Collectors.toList()));
    }

    /**
     * Creates a {@link DbContextBulkUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     * for a list of objects. Example:
     *
     * <pre>
     *     public void updateAll(List&lt;TagType&gt; tagTypes) {
     *         tagTypesTable.bulkUpdate(tagTypes)
     *              .where("id", TagType::getId)
     *              .setField("name", TagType::getName)
     *              .execute();
     *     }
     * </pre>
     */
    private <T> DbContextBulkUpdateBuilder<T> bulkUpdate(List<T> objects) {
        return new DbContextBulkUpdateBuilder<>(this, table.bulkUpdate(objects));
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
