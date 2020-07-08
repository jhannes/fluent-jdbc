package org.fluentjdbc;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * <p>Provides a starting point for for fluent-jdbc with explicit Connection management
 * </p>
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
 *
 * @see DatabaseTableImpl
 * @see DatabaseTableWithTimestamps
 */
@ParametersAreNonnullByDefault
public interface DatabaseTable extends DatabaseQueryable<DatabaseSimpleQueryBuilder> {

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    DatabaseListableQueryBuilder unordered();

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    DatabaseListableQueryBuilder orderBy(String orderByClause);

    DatabaseTableAlias alias(String alias);

    @FunctionalInterface
    interface RowMapper<T> {
        T mapRow(DatabaseRow row) throws SQLException;
    }

    @FunctionalInterface
    interface RowConsumer {
        void apply(DatabaseRow row) throws SQLException;
    }

    String getTableName();

    /**
     * Creates a {@link DatabaseSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database. If idValue is null,
     * {@link DatabaseSaveBuilder} will attempt to use the table's autogeneration of primary keys
     * if there is no row with matching unique keys
     */
    DatabaseSaveBuilder<Long> newSaveBuilder(String idColumn, @Nullable Long idValue);

    /**
     * Creates a {@link DatabaseSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database. Throws exception if idValue is null
     */
    DatabaseSaveBuilder<String> newSaveBuilderWithString(String idColumn, String idValue);

    /**
     * Use instead of {@link #newSaveBuilder} if the database driver does not
     * support RETURN_GENERATED_KEYS
     */
    DatabaseSaveBuilder<Long> newSaveBuilderNoGeneratedKeys(String idColumn, @Nullable Long idValue);

    /**
     * Creates a {@link DatabaseSaveBuilder} which creates a <code>INSERT</code> or <code>UPDATE</code>
     * statement, depending on whether the row already exists in the database.
     * Generates UUID.randomUUID if idValue is null and row with matching unique keys does not already exist
     */
    DatabaseSaveBuilder<UUID> newSaveBuilderWithUUID(String fieldName, @Nullable UUID uuid);

    /**
     * Creates a {@link DatabaseInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement
     */
    DatabaseInsertBuilder insert();

    /**
     * Creates a {@link DatabaseUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    DatabaseUpdateBuilder update();

    /**
     * Executes <code>DELETE FROM tableName</code>
     */
    DatabaseDeleteBuilder delete();

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
     *
     */
    <T> DatabaseBulkInsertBuilder<T> bulkInsert(Iterable<T> objects);

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
    <T> DatabaseBulkInsertBuilder<T> bulkInsert(Stream<T> objects);

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
    <T> DatabaseBulkDeleteBuilder<T> bulkDelete(Iterable<T> objects);

    /**
     * Creates a {@link DatabaseBulkUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     * for a list of objects. Example:
     *
     * <pre>
     *     public void updateAll(List&lt;TagType&gt; tagTypes, Connection connection) {
     *         tagTypesTable.bulkUpdate(tagTypes)
     *              .where("id", TagType::getId)
     *              .setField("name", TagType::getName)
     *              .execute(connection);
     *     }
     * </pre>
     */
    <T> DatabaseBulkUpdateBuilder<T> bulkUpdate(Iterable<T> objects);

}
