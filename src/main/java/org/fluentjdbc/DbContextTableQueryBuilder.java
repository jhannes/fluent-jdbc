package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.sql.Connection;
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
public class DbContextTableQueryBuilder implements DbContextListableSelect<DbContextTableQueryBuilder> {

    private final DbContextTable dbContextTable;
    private final DatabaseTableQueryBuilder builder;

    public DbContextTableQueryBuilder(DbContextTable dbContextTable) {
        this.dbContextTable = dbContextTable;
        builder = new DatabaseTableQueryBuilder(dbContextTable.getTable());
    }

    /**
     * Returns this. Needed to make {@link DbContextTableQueryBuilder} interchangeable with {@link DbContextTable}
     */
    @Override
    public DbContextTableQueryBuilder query() {
        return this;
    }


    @CheckReturnValue
    private DbContextTableQueryBuilder query(@SuppressWarnings("unused") DatabaseTableQueryBuilder builder) {
        return this;
    }

    /**
     * Add the arguments to the column list for the <code>SELECT column, column...</code> statement
     */
    @CheckReturnValue
    public DbContextSelectBuilder select(String... columns) {
        return new DbContextSelectBuilder(dbContextTable.getDbContext(), builder.select(columns));
    }

    /**
     * Adds the parameter to the WHERE-clause and all the parameter list.
     * E.g. <code>where(new DatabaseQueryParameter("created_at between ? and ?", List.of(earliestDate, latestDate)))</code>
     */
    @Override
    public DbContextTableQueryBuilder where(DatabaseQueryParameter parameter) {
        return query(builder.where(parameter));
    }

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @Override
    public DbContextTableQueryBuilder orderBy(String orderByClause) {
        return query(builder.orderBy(orderByClause));
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link #list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @CheckReturnValue
    public DbContextTableQueryBuilder unordered() {
        return query(builder.unordered());
    }

    /**
     * Adds <code>FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code> statement.
     * FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @CheckReturnValue
    public DbContextSelectBuilder limit(int rowCount) {
        return skipAndLimit(0, rowCount);
    }

    /**
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @CheckReturnValue
    public DbContextSelectBuilder skipAndLimit(int offset, int rowCount) {
        return new DbContextSelectBuilder(dbContextTable.getDbContext(), builder.skipAndLimit(offset, rowCount));
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <OBJECT> Stream<OBJECT> stream(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.stream(getConnection(), mapper);
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <OBJECT> List<OBJECT> list(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.list(getConnection(), mapper);
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @Override
    public int getCount() {
        return builder.getCount(getConnection());
    }

    /**
     * If the query returns no rows, returns {@link SingleRow#absent}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param mapper Function object to map a single returned row to an object
     * @return the mapped row if one row is returned, {@link SingleRow#absent} otherwise
     * @throws MultipleRowsReturnedException if more than one row was matched the query
     */
    @Nonnull
    @Override
    public <OBJECT> SingleRow<OBJECT> singleObject(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.singleObject(getConnection(), mapper);
    }

    /**
     * Returns a string from the specified column name
     *
     * @return the mapped row if one row is returned, {@link SingleRow#absent} otherwise
     * @throws MultipleRowsReturnedException if more than one row was matched the query
     */
    @Nonnull
    @Override
    public SingleRow<String> singleString(String fieldName) {
        return builder.singleString(getConnection(), fieldName);
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    @Override
    public void forEach(DatabaseResult.RowConsumer consumer) {
        builder.forEach(getConnection(), consumer);
    }

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code>
     */
    public int executeDelete() {
        return builder.delete(getConnection());
    }

    /**
     * Creates a {@link DbContextUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    @CheckReturnValue
    public DbContextUpdateBuilder update() {
        return new DbContextUpdateBuilder(this.dbContextTable, builder.update());
    }

    @CheckReturnValue
    public DbContextInsertOrUpdateBuilder insertOrUpdate() {
        return new DbContextInsertOrUpdateBuilder(this.dbContextTable, builder.insertOrUpdate());
    }

    @CheckReturnValue
    private Connection getConnection() {
        return dbContextTable.getConnection();
    }
}
