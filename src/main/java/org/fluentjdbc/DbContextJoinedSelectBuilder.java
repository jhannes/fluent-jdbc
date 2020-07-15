package org.fluentjdbc;

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
public class DbContextJoinedSelectBuilder implements DbContextListableSelect<DbContextJoinedSelectBuilder> {
    private final DbContext dbContext;
    private final DatabaseJoinedQueryBuilder builder;

    public DbContextJoinedSelectBuilder(DbContextTableAlias table) {
        dbContext = table.getDbContext();
        builder = new DatabaseJoinedQueryBuilder(table.getTableAlias());
    }

    /**
     * Adds an additional table to the join as an inner join. Inner joins require a matching row
     * in both tables and will leave out rows from one of the table where there is no corresponding
     * table in the other
     */
    public DbContextJoinedSelectBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        builder.join(a, b);
        return this;
    }

    /**
     * Adds an additional table to the join as a left join. Left join only require a matching row in
     * the first/left table. If there is no matching row in the second/right table, all columns are
     * returned as null in this table. When calling {@link DatabaseRow#table(DatabaseTableAlias)} on
     * the resulting row, <code>null</code> is returned
     */
    public DbContextJoinedSelectBuilder leftJoin(DatabaseColumnReference a, DatabaseColumnReference b) {
        builder.leftJoin(a, b);
        return this;
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
    @Override
    public DbContextJoinedSelectBuilder whereExpression(String expression, @Nullable Object value) {
        builder.whereExpression(expression, value);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    public DbContextJoinedSelectBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters){
        builder.whereExpressionWithMultipleParameters(expression, parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause
     */
    @Override
    public DbContextJoinedSelectBuilder whereExpression(String expression) {
        builder.whereExpression(expression);
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @Override
    public DbContextJoinedSelectBuilder whereOptional(String fieldName, @Nullable Object value) {
        builder.whereOptional(fieldName, value);
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    @Override
    public DbContextJoinedSelectBuilder whereIn(String fieldName, Collection<?> parameters) {
        builder.whereIn(fieldName, parameters);
        return this;
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DbContextJoinedSelectBuilder whereAll(List<String> fields, List<Object> values) {
        builder.whereAll(fields, values);
        return this;
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
        builder.unordered();
        return this;
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    public DbContextJoinedSelectBuilder orderBy(String orderByClause) {
        builder.orderBy(orderByClause);
        return this;
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    public DbContextJoinedSelectBuilder orderBy(DatabaseColumnReference column) {
        return orderBy(column.getQualifiedColumnName());
    }

    private Connection getConnection() {
        return dbContext.getThreadConnection();
    }
}

