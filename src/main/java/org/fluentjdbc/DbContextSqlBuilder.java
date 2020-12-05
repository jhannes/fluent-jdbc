package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class DbContextSqlBuilder implements DbContextListableSelect<DbContextSqlBuilder> {

    private final DatabaseSqlBuilder builder;
    private final DbContext dbContext;

    public DbContextSqlBuilder(DbContext dbContext) {
        this.dbContext = dbContext;
        builder = new DatabaseSqlBuilder(dbContext.tableReporter("<query>"));
    }

    @Override
    public DbContextSqlBuilder query() {
        return this;
    }
    @Override
    public DbContextSqlBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        return query().whereExpressionWithParameterList(expression, parameters);
    }

    public DbContextSqlBuilder select(String... columns) {
        return query(builder.select(columns));
    }

    public DbContextSqlBuilder from(String fromStatement) {
        return query(builder.from(fromStatement));
    }


    public DbContextSqlBuilder groupBy(String... groupByStatement) {
        return query(builder.groupBy(groupByStatement));
    }

    @Override
    public DbContextSqlBuilder orderBy(String orderByClause) {
        return query(builder.orderBy(orderByClause));
    }

    public DbContextSqlBuilder unordered() {
        return query(builder.unordered());
    }

    @Override
    public DbContextSqlBuilder limit(int rowCount) {
        return query(builder.limit(rowCount));
    }

    @Override
    public DbContextSqlBuilder skipAndLimit(int offset, int rowCount) {
        return query(builder.skipAndLimit(offset, rowCount));
    }

    @Override
    public <OBJECT> Stream<OBJECT> stream(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.stream(getConnection(), mapper);
    }

    @Override
    public <OBJECT> List<OBJECT> list(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.list(getConnection(), mapper);
    }

    @Override
    public int getCount() {
        return builder.getCount(getConnection());
    }

    @Nonnull
    @Override
    public <OBJECT> Optional<OBJECT> singleObject(DatabaseResult.RowMapper<OBJECT> mapper) {
        return builder.singleObject(getConnection(), mapper);
    }

    @Override
    public void forEach(DatabaseResult.RowConsumer consumer) {
        builder.forEach(getConnection(), consumer);
    }

    public Connection getConnection() {
        return dbContext.getThreadConnection();
    }

    @SuppressWarnings("unused")
    private DbContextSqlBuilder query(DatabaseSqlBuilder builder) {
        return this;
    }

}
