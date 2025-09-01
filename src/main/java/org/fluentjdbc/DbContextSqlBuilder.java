package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Used to construct SQL SELECT statements in a flexible way with {@link #where(String, Object)}
 * clauses, {@link #select(String...)} column names, {@link #from(String)} table statement,
 * {@link #groupBy(String...)}, {@link #orderBy(String)} and {@link #skipAndLimit(int, int)}.
 *
 * <h2>Example:</h2>
 *
 * <pre>
 * new DbContextSqlBuilder(dbContext)
 *      .select("first_name", "last_name")
 *      .select("p.id")
 *      .from("person p full join organizations o")
 *      .where("organization_sector", sector)
 *      .limit(100)
 *      .list(row -&gt; List.of(row.getString("first_name"), row.getString("last_name"), row.getUUID("p.id"));
 * </pre>
 *
 */
public class DbContextSqlBuilder implements DbContextListableSelect<DbContextSqlBuilder> {

    private final DatabaseSqlBuilder builder;
    private final DbContext dbContext;

    public DbContextSqlBuilder(DbContext dbContext) {
        this.dbContext = dbContext;
        builder = new DatabaseSqlBuilder(dbContext.getStatementFactory());
    }

    /**
     * Implemented as <code>return this</code> for compatibility purposes
     */
    @Override
    public DbContextSqlBuilder query() {
        return this;
    }

    /**
     * Add the arguments to the column list for the <code>SELECT column, column...</code> statement
     */
    @CheckReturnValue
    public DbContextSqlBuilder select(String... columns) {
        return query(builder.select(columns));
    }

    /**
     * Replace the "from" part of the <code>SELECT ... FROM fromStatement</code> in the select statement
     */
    @CheckReturnValue
    public DbContextSqlBuilder from(String fromStatement) {
        return query(builder.from(fromStatement));
    }

    @Override
    public DbContextSqlBuilder where(DatabaseQueryParameter parameter) {
        builder.where(parameter);
        return this;
    }

    /**
     * Add the arguments to the column list for the <code>SELECT ... FROM ... ... GROUP BY groupByStatement</code> statement
     */
    @CheckReturnValue
    public DbContextSqlBuilder groupBy(String... groupByStatement) {
        return query(builder.groupBy(groupByStatement));
    }

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @Override
    public DbContextSqlBuilder orderBy(String orderByClause) {
        return query(builder.orderBy(orderByClause));
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @CheckReturnValue
    public DbContextSqlBuilder unordered() {
        return query(builder.unordered());
    }

    /**
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @Override
    public DbContextSqlBuilder skipAndLimit(int offset, int rowCount) {
        return query(builder.skipAndLimit(offset, rowCount));
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
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    @Override
    public void forEach(DatabaseResult.RowConsumer consumer) {
        builder.forEach(getConnection(), consumer);
    }

    @SuppressWarnings("unused")
    private DbContextSqlBuilder query(DatabaseSqlBuilder builder) {
        return this;
    }

    @Nonnull
    private Connection getConnection() {
        return dbContext.getThreadConnection();
    }
}
