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

    @Override
    public int getCount() {
        return builder.getCount(getConnection());
    }

    @Override
    public <OBJECT> Stream<OBJECT> stream(DatabaseTable.RowMapper<OBJECT> mapper) {
        return builder.stream(getConnection(), mapper);
    }

    @Override
    public <OBJECT> List<OBJECT> list(DatabaseTable.RowMapper<OBJECT> mapper) {
        return builder.list(getConnection(), mapper);
    }

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

    @Override
    public DbJoinedSelectContext query() {
        return this;
    }

    @Override
    @Nonnull
    public <OBJECT> Optional<OBJECT> singleObject(DatabaseTable.RowMapper<OBJECT> mapper) {
        return builder.singleObject(getConnection(), mapper);
    }

    public DbJoinedSelectContext unordered() {
        builder.unordered();
        return this;
    }

    public DbJoinedSelectContext orderBy(String orderByClause) {
        builder.orderBy(orderByClause);
        return this;
    }

    public DbJoinedSelectContext orderBy(DatabaseColumnReference column) {
        return orderBy(column.getQualifiedColumnName());
    }

    private Connection getConnection() {
        return dbContext.getThreadConnection();
    }
}

