package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class DbSelectContext implements DbListableSelectContext<DbSelectContext> {

    private DbTableContext dbTableContext;
    private DatabaseTableQueryBuilder queryBuilder;

    public DbSelectContext(DbTableContext dbTableContext) {
        this.dbTableContext = dbTableContext;
        queryBuilder = new DatabaseTableQueryBuilder(dbTableContext.getTable());
    }

    @Override
    public DbSelectContext query() {
        return this;
    }

    @Override
    public DbSelectContext whereOptional(String fieldName, @Nullable Object value) {
        queryBuilder.whereOptional(fieldName, value);
        return this;
    }

    @Override
    public DbSelectContext whereExpression(String expression) {
        queryBuilder.whereExpression(expression);
        return this;
    }

    @Override
    public DbSelectContext whereExpression(String expression, Object value) {
        queryBuilder.whereExpression(expression, value);
        return this;
    }

    @Override
    public DbSelectContext whereIn(String fieldName, Collection<?> parameters) {
        queryBuilder.whereIn(fieldName, parameters);
        return this;
    }

    @Override
    public DbSelectContext whereExpressionWithMultipleParameters(String expression, Collection<?> parameters) {
        queryBuilder.whereExpressionWithMultipleParameters(expression, parameters);
        return this;
    }

    @Override
    public DbSelectContext whereAll(List<String> fields, List<Object> values) {
        queryBuilder.whereAll(fields, values);
        return this;
    }

    public DbSelectContext orderBy(String orderByClause) {
        queryBuilder.orderBy(orderByClause);
        return this;
    }

    public DbSelectContext unordered() {
        queryBuilder.unordered();
        return this;
    }

    private Connection getConnection() {
        return dbTableContext.getConnection();
    }

    public void executeDelete() {
        queryBuilder.delete(getConnection());
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

    @Nonnull
    @Override
    public Optional<Number> singleLong(String fieldName) {
        return queryBuilder.singleLong(getConnection(), fieldName);
    }

    @Nonnull
    @Override
    public Optional<Instant> singleInstant(String fieldName) {
        return queryBuilder.singleInstant(getConnection(), fieldName);
    }

    @Override
    public void forEach(DatabaseTable.RowConsumer row) {
        queryBuilder.forEach(getConnection(), row);
    }

    public DbContextUpdateBuilder update() {
        return new DbContextUpdateBuilder(this.dbTableContext, queryBuilder.update());
    }

}
