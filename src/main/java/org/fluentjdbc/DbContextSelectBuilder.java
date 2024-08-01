package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Generate <code>SELECT</code> statements by collecting <code>WHERE</code> expressions and parameters.Example:
 *
 * <pre>
 * {@link DbContextTable} table = context.table("database_test_table");
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *      List&lt;Person&gt; result = table
 *          .where("firstName", firstName)
 *          .whereExpression("lastName like ?", "joh%")
 *          .whereIn("status", statuses)
 *          .orderBy("lastName")
 *          .list(row -&gt; new Person(row));
 * }
 * </pre>
 *
 * @see org.fluentjdbc.DatabaseTableQueryBuilder
 */
public class DbContextSelectBuilder implements DbContextListableSelect<DbContextSelectBuilder> {

    private final DbContextTable dbContextTable;
    private final DatabaseTableQueryBuilder queryBuilder;

    public DbContextSelectBuilder(DbContextTable dbContextTable) {
        this.dbContextTable = dbContextTable;
        queryBuilder = new DatabaseTableQueryBuilder(dbContextTable.getTable());
    }

    /**
     * Returns this. Needed to make {@link DbContextSelectBuilder} interchangeable with {@link DbContextTable}
     */
    @Override
    public DbContextSelectBuilder query() {
        return this;
    }

    @CheckReturnValue
    private DbContextSelectBuilder query(@SuppressWarnings("unused") DatabaseTableQueryBuilder builder) {
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpressionWithParameterList("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    @Override
    public DbContextSelectBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        return query(queryBuilder.whereExpressionWithParameterList(expression, parameters));
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereColumnValues("json_column", "?::json", jsonString)</code>
     */
    @CheckReturnValue
    public DbContextSelectBuilder whereColumnValuesEqual(String column, String expression, Collection<?> parameters) {
        return query(queryBuilder.whereColumnValuesEqual(column, expression, parameters));
    }

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @Override
    public DbContextSelectBuilder orderBy(String orderByClause) {
        return query(queryBuilder.orderBy(orderByClause));
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link #list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @CheckReturnValue
    public DbContextSelectBuilder unordered() {
        return query(queryBuilder.unordered());
    }

    /**
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @Override
    public DbContextSelectBuilder skipAndLimit(int offset, int rowCount) {
        return query(queryBuilder.skipAndLimit(offset, rowCount));
    }

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code>
     */
    public int executeDelete() {
        return queryBuilder.delete(getConnection());
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> Stream<T> stream(DatabaseResult.RowMapper<T> mapper) {
        return queryBuilder.stream(getConnection(), mapper);
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> List<T> list(DatabaseResult.RowMapper<T> mapper) {
        return queryBuilder.list(getConnection(), mapper);
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @Override
    public int getCount() {
        return queryBuilder.getCount(getConnection());
    }

    /**
     * If the query returns no rows, returns {@link SingleRow#absent}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param mapper Function object to map a single returned row to an object
     * @return the mapped row if one row is returned, {@link SingleRow#absent} otherwise
     * @throws IllegalStateException if more than one row was matched the query
     */
    @Nonnull
    @Override
    public <T> SingleRow<T> singleObject(DatabaseResult.RowMapper<T> mapper) {
        return queryBuilder.singleObject(getConnection(), mapper);
    }

    /**
     * Returns a string from the specified column name
     *
     * @return the mapped row if one row is returned, {@link SingleRow#absent} otherwise
     * @throws IllegalStateException if more than one row was matched the query
     */
    @Nonnull
    @Override
    public SingleRow<String> singleString(String fieldName) {
        return queryBuilder.singleString(getConnection(), fieldName);
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    @Override
    public void forEach(DatabaseResult.RowConsumer consumer) {
        queryBuilder.forEach(getConnection(), consumer);
    }

    /**
     * Creates a {@link DbContextUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    @CheckReturnValue
    public DbContextUpdateBuilder update() {
        return new DbContextUpdateBuilder(this.dbContextTable, queryBuilder.update());
    }

    @CheckReturnValue
    public DbContextInsertOrUpdateBuilder insertOrUpdate() {
        return new DbContextInsertOrUpdateBuilder(this.dbContextTable, queryBuilder.insertOrUpdate());
    }

    @CheckReturnValue
    private Connection getConnection() {
        return dbContextTable.getConnection();
    }
}
