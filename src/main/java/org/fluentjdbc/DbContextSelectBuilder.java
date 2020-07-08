package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;

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
 * DbTableContext table = context.table("database_test_table");
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
public class DbContextSelectBuilder implements DbListableSelectContext<DbContextSelectBuilder> {

    private final DbTableContext dbTableContext;
    private final DatabaseTableQueryBuilder queryBuilder;

    public DbContextSelectBuilder(DbTableContext dbTableContext) {
        this.dbTableContext = dbTableContext;
        queryBuilder = new DatabaseTableQueryBuilder(dbTableContext.getTable());
    }

    /**
     * Returns this. Needed to make {@link DbContextSelectBuilder} interchangeable with {@link DbTableContext}
     */
    @Override
    public DbContextSelectBuilder query() {
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @Override
    public DbContextSelectBuilder whereOptional(String fieldName, @Nullable Object value) {
        queryBuilder.whereOptional(fieldName, value);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause
     */
    @Override
    public DbContextSelectBuilder whereExpression(String expression) {
        queryBuilder.whereExpression(expression);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and the value to the parameter list. E.g.
     * <code>whereExpression("created_at &gt; ?", earliestDate)</code>
     */
    @Override
    public DbContextSelectBuilder whereExpression(String expression, Object value) {
        queryBuilder.whereExpression(expression, value);
        return this;
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
    public DbContextSelectBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters) {
        queryBuilder.whereExpressionWithMultipleParameters(expression, parameters);
        return this;
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DbContextSelectBuilder whereAll(List<String> fields, List<Object> values) {
        queryBuilder.whereAll(fields, values);
        return this;
    }

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    public DbContextSelectBuilder orderBy(String orderByClause) {
        queryBuilder.orderBy(orderByClause);
        return this;
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link #list(RowMapper)}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    public DbContextSelectBuilder unordered() {
        queryBuilder.unordered();
        return this;
    }

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code>
     */
    public int executeDelete() {
        return queryBuilder.delete(getConnection());
    }

    public <T> Stream<T> stream(RowMapper<T> mapper) {
        return queryBuilder.stream(getConnection(), mapper);
    }

    @Override
    public <T> List<T> list(RowMapper<T> mapper) {
        return queryBuilder.list(getConnection(), mapper);
    }

    @Override
    public int getCount() {
        return queryBuilder.getCount(getConnection());
    }

    @Nonnull
    @Override
    public <T> Optional<T> singleObject(RowMapper<T> mapper) {
        return queryBuilder.singleObject(getConnection(), mapper);
    }

    @Nonnull
    @Override
    public Optional<String> singleString(String fieldName) {
        return queryBuilder.singleString(getConnection(), fieldName);
    }

    @Override
    public void forEach(DatabaseTable.RowConsumer row) {
        queryBuilder.forEach(getConnection(), row);
    }

    /**
     * Creates a {@link DbContextUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    public DbContextUpdateBuilder update() {
        return new DbContextUpdateBuilder(this.dbTableContext, queryBuilder.update());
    }

    private Connection getConnection() {
        return dbTableContext.getConnection();
    }
}
