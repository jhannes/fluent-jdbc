package org.fluentjdbc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class DbJoinedSelectContext implements DbListableSelectContext<DbJoinedSelectContext> {
    private final DbContext dbContext;
    private final DatabaseJoinedQueryBuilder builder;

    public DbJoinedSelectContext(DbTableAliasContext dbTableContext) {
        dbContext = dbTableContext.getDbContext();
        builder = new DatabaseJoinedQueryBuilder(dbTableContext.getTableAlias());
    }

    public DbJoinedSelectContext join(DatabaseColumnReference a, DatabaseColumnReference b) {
        builder.join(a, b);
        return this;
    }

    public DbJoinedSelectContext leftJoin(DatabaseColumnReference a, DatabaseColumnReference b) {
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
     * Execute the query and map each return value over the {@link DatabaseTable.RowMapper} function to return a stream. Example:
     *
     * <pre>
     *     t.join(t.column("id"), o.column("parent_id"))
     *          .where("status", status)
     *          .stream(row -> row.table(joinedTable).getInstant("created_at"))
     * </pre>
     */
    @Override
    public <OBJECT> Stream<OBJECT> stream(DatabaseTable.RowMapper<OBJECT> mapper) {
        return builder.stream(getConnection(), mapper);
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseTable.RowMapper} function to return a list. Example:
     *
     * <pre>
     *     DbTableAliasContext t = table.alias("t");
     *     DbTableAliasContext o = otherTable.alias("o");
     *     List&lt;Instant&gt; creationTimes = t.join(t.column("id"), o.column("parent_id"))
     *          .where(t.column("name"), name)
     *          .list(row -> row.table(o).getInstant("created_at"));
     * </pre>
     */
    @Override
    public <OBJECT> List<OBJECT> list(DatabaseTable.RowMapper<OBJECT> mapper) {
        return builder.list(getConnection(), mapper);
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link org.fluentjdbc.DatabaseTable.RowConsumer} for each returned row
     */
    @Override
    public void forEach(DatabaseTable.RowConsumer consumer) {
        builder.forEach(getConnection(), consumer);
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    @Override
    public DbJoinedSelectContext whereExpression(String expression, @Nullable Object value) {
        builder.whereExpression(expression, value);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    public DbJoinedSelectContext whereExpressionWithMultipleParameters(String expression, Collection<?> parameters){
        builder.whereExpressionWithMultipleParameters(expression, parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause
     */
    @Override
    public DbJoinedSelectContext whereExpression(String expression) {
        builder.whereExpression(expression);
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @Override
    public DbJoinedSelectContext whereOptional(String fieldName, @Nullable Object value) {
        builder.whereOptional(fieldName, value);
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    @Override
    public DbJoinedSelectContext whereIn(String fieldName, Collection<?> parameters) {
        builder.whereIn(fieldName, parameters);
        return this;
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DbJoinedSelectContext whereAll(List<String> fields, List<Object> values) {
        builder.whereAll(fields, values);
        return this;
    }

    /**
     * Returns or creates a query object to be used to add {@link #where(String, Object)} statements and operations
     */
    @Override
    public DbJoinedSelectContext query() {
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
    public <OBJECT> Optional<OBJECT> singleObject(DatabaseTable.RowMapper<OBJECT> mapper) {
        return builder.singleObject(getConnection(), mapper);
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    public DbJoinedSelectContext unordered() {
        builder.unordered();
        return this;
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    public DbJoinedSelectContext orderBy(String orderByClause) {
        builder.orderBy(orderByClause);
        return this;
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    public DbJoinedSelectContext orderBy(DatabaseColumnReference column) {
        return orderBy(column.getQualifiedColumnName());
    }

    private Connection getConnection() {
        return dbContext.getThreadConnection();
    }
}

