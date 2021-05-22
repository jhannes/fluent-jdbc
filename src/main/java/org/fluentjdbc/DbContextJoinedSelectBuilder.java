package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * {@link DbContextSelectBuilder} used to generate joined queries using SQL-92 standard
 * <code>SELECT * FROM table1 a JOIN table2 b ON a.column = b.column</code>. To specify
 * columns for selection and tables for retrieval of columns, use {@link DbContextTableAlias}
 * and {@link DatabaseColumnReference}.
 *
 * <p><strong>Only works well on JDBC drivers that implement {@link ResultSetMetaData#getTableName(int)}</strong>,
 * as this is used to calculate column indexes for aliased tables. This includes PostgreSQL, H2, HSQLDB, and SQLite,
 * but not Oracle or SQL Server.</p>
 *
 * <p>Pull requests are welcome for a substitute for SQL Server and Oracle.</p>
 *
 * <h3>Usage example:</h3>

 * <pre>
 * {@link DbContextTable}Alias p = productsTable.alias("p");
 * DbContextTableAlias o = ordersTable.alias("o");
 * return context
 *         .join(linesAlias.column("product_id"), p.column("product_id"))
 *         .join(linesAlias.column("order_id"), o.column("order_id"))
 *         .list(row -&gt; new OrderLineEntity(
 *                 OrderRepository.toOrder(row.table(o)),
 *                 ProductRepository.toProduct(row.table(p)),
 *                 toOrderLine(row.table(linesAlias))
 *         ));
 * </pre>
 */
@CheckReturnValue
public class DbContextJoinedSelectBuilder implements DbContextListableSelect<DbContextJoinedSelectBuilder> {
    private final DbContext dbContext;
    private final DatabaseJoinedQueryBuilder builder;

    public DbContextJoinedSelectBuilder(@Nonnull DbContextTableAlias table) {
        dbContext = table.getDbContext();
        builder = new DatabaseJoinedQueryBuilder(table.getTableAlias(), table.getReporter().operation("SELECT"));
    }

    /**
     * Adds an additional table to the join as an inner join. Inner joins require a matching row
     * in both tables and will leave out rows from one of the table where there is no corresponding
     * table in the other
     */
    public DbContextJoinedSelectBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        return query(builder.join(a, b));
    }

    /**
     * Adds an additional table to the join as an inner join. Inner joins require that all columns
     * in both tables match and will leave out rows from one of the table where there is no corresponding
     * table in the other
     */
    public DbContextJoinedSelectBuilder join(List<String> leftFields, DbContextTableAlias joinedTable, List<String> rightFields) {
        return query(builder.join(leftFields, joinedTable.getTableAlias(), rightFields));
    }

    /**
     * Adds an additional table to the join as a left join. Left join only require a matching row in
     * the first/left table. If there is no matching row in the second/right table, all columns are
     * returned as null in this table. When calling {@link DatabaseRow#table(DatabaseTableAlias)} on
     * the resulting row, <code>null</code> is returned
     */
    public DbContextJoinedSelectBuilder leftJoin(DatabaseColumnReference a, DatabaseColumnReference b) {
        return query(builder.leftJoin(a, b));
    }

    /**
     * Adds an additional table to the join as a left join. Left join only require a matching row in
     * the first/left table. If there is no matching row in the second/right table, all columns are
     * returned as null in this table. When calling {@link DatabaseRow#table(DatabaseTableAlias)} on
     * the resulting row, <code>null</code> is returned
     */
    public DbContextJoinedSelectBuilder leftJoin(List<String> leftFields, DbContextTableAlias joinedTable, List<String> rightFields) {
        return query(builder.leftJoin(leftFields, joinedTable.getTableAlias(), rightFields));
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @Override
    public int getCount() {
        return builder.getCount(getConnection());
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     *
     * <pre>
     *     t.join(t.column("id"), o.column("parent_id"))
     *          .where("status", status)
     *          .stream(row -&gt; row.table(joinedTable).getInstant("created_at"))
     * </pre>
     */
    @Override
    public <OBJECT> Stream<OBJECT> stream(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.stream(getConnection(), mapper);
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     *
     * <pre>
     *     {@link DbContextTableAlias} t = table.alias("t");
     *     DbContextTableAlias o = otherTable.alias("o");
     *     List&lt;Instant&gt; creationTimes = t.join(t.column("id"), o.column("parent_id"))
     *          .where(t.column("name"), name)
     *          .list(row -&gt; row.table(o).getInstant("created_at"));
     * </pre>
     */
    @Override
    public <OBJECT> List<OBJECT> list(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.list(getConnection(), mapper);
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
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    public DbContextJoinedSelectBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters){
        return query(builder.whereExpressionWithParameterList(expression, parameters));
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @Override
    public DbContextJoinedSelectBuilder whereOptional(String fieldName, @Nullable Object value) {
        return query(builder.whereOptional(fieldName, value));
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DbContextJoinedSelectBuilder whereAll(List<String> fields, List<Object> values) {
        return query(builder.whereAll(fields, values));
    }

    /**
     * Returns or creates a query object to be used to add {@link #where(String, Object)} statements and operations
     */
    @Override
    public DbContextJoinedSelectBuilder query() {
        return this;
    }

    /**
     * If the query returns no rows, returns {@link Optional#empty()}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param mapper Function object to map a single returned row to a object
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Override
    @Nonnull
    public <OBJECT> Optional<OBJECT> singleObject(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.singleObject(getConnection(), mapper);
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    public DbContextJoinedSelectBuilder unordered() {
        return query(builder.unordered());
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    @Override
    public DbContextJoinedSelectBuilder orderBy(String orderByClause) {
        return query(builder.orderBy(orderByClause));
    }

    /**
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @Override
    public DbContextJoinedSelectBuilder skipAndLimit(int offset, int rowCount) {
        return query(builder.skipAndLimit(offset, rowCount));
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    public DbContextJoinedSelectBuilder orderBy(DatabaseColumnReference column) {
        return orderBy(column.getQualifiedColumnName());
    }

    private DbContextJoinedSelectBuilder query(@SuppressWarnings("unused") DatabaseJoinedQueryBuilder builder) {
        return this;
    }

    private Connection getConnection() {
        return dbContext.getThreadConnection();
    }
}

