package org.fluentjdbc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
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
        queryBuilder = new DatabaseTableQueryBuilder(dbContextTable.getTable(), dbContextTable.getReporter());
    }

    /**
     * Returns this. Needed to make {@link DbContextSelectBuilder} interchangeable with {@link DbContextTable}
     */
    @Override
    public DbContextSelectBuilder query() {
        return this;
    }

    private DbContextSelectBuilder query(DatabaseTableQueryBuilder builder) {
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @Override
    public DbContextSelectBuilder whereOptional(String fieldName, @Nullable Object value) {
        return query(queryBuilder.whereOptional(fieldName, value));
    }

    /**
     * Adds the expression to the WHERE-clause
     */
    @Override
    public DbContextSelectBuilder whereExpression(String expression) {
        return query(queryBuilder.whereExpression(expression));
    }

    /**
     * Adds the expression to the WHERE-clause and the value to the parameter list. E.g.
     * <code>whereExpression("created_at &gt; ?", earliestDate)</code>
     */
    @Override
    public DbContextSelectBuilder whereExpression(String expression, Object parameter) {
        return query(queryBuilder.whereExpression(expression, parameter));
    }

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    @Override
    public DbContextSelectBuilder whereIn(String fieldName, Collection<?> parameters) {
        queryBuilder.whereIn(fieldName, parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    @Override
    public DbContextSelectBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        return query(queryBuilder.whereExpressionWithParameterList(expression, parameters));
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DbContextSelectBuilder whereAll(List<String> fields, List<Object> values) {
        return query(queryBuilder.whereAll(fields, values));
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
    public DbContextSelectBuilder unordered() {
        return query(queryBuilder.unordered());
    }

    /**
     * Adds <code>FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code> statement.
     * FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @Override
    public DbContextSelectBuilder limit(int rowCount) {
        return query(queryBuilder.limit(rowCount));
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
     * If the query returns no rows, returns {@link Optional#empty()}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param mapper Function object to map a single returned row to a object
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @Override
    public <T> Optional<T> singleObject(DatabaseResult.RowMapper<T> mapper) {
        return queryBuilder.singleObject(getConnection(), mapper);
    }

    /**
     * Returns a string from the specified column name
     *
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @Override
    public Optional<String> singleString(String fieldName) {
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
    public DbContextUpdateBuilder update() {
        return new DbContextUpdateBuilder(this.dbContextTable, queryBuilder.update());
    }

    private Connection getConnection() {
        return dbContextTable.getConnection();
    }
}
