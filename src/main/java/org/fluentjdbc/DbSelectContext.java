package org.fluentjdbc;

import java.sql.Connection;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.fluentjdbc.DatabaseTable.RowMapper;

public class DbSelectContext implements DbListableSelectContext {

    private DbTableContext dbTableContext;
    private DatabaseQueryBuilder queryBuilder;

    public DbSelectContext(DbTableContext dbTableContext) {
        this.dbTableContext = dbTableContext;
        queryBuilder = new DatabaseQueryBuilder(dbTableContext.getTable());
    }

    public DbSelectContext where(String fieldName, Object value) {
        queryBuilder.where(fieldName, value);
        return this;
    }

    public DbSelectContext whereOptional(String fieldName, Object value) {
        queryBuilder.whereOptional(fieldName, value);
        return this;
    }

    public DbSelectContext whereExpression(String expression) {
        queryBuilder.whereExpression(expression);
        return this;
    }

    public DbSelectContext whereIn(String fieldName, Collection<?> parameters) {
        queryBuilder.whereIn(fieldName, parameters);
        return this;
    }

    public DbSelectContext orderBy(String orderByClause) {
        queryBuilder.orderBy(orderByClause);
        return this;
    }

    public DbListableSelectContext unordered() {
        queryBuilder.unordered();
        return this;
    }

    public List<Long> listLongs(String fieldName) {
        return queryBuilder.listLongs(getConnection(), fieldName);
    }

    private Connection getConnection() {
        return dbTableContext.getConnection();
    }

    @Override
    public List<String> listStrings(String fieldName) {
        return queryBuilder.listStrings(getConnection(), fieldName);
    }

    public void executeDelete() {
        queryBuilder.delete(getConnection());
    }

    public <T> Stream<T> stream(RowMapper<T> mapper) {
        return queryBuilder.stream(getConnection(), mapper);
    }

    @Override
    public <T> List<T> list(RowMapper<T> mapper) {
        return listObjects(mapper);
    }

    public <T> List<T> listObjects(RowMapper<T> mapper) {
        return queryBuilder.list(getConnection(), mapper);
    }

    public <T> T singleObject(RowMapper<T> mapper) {
        return queryBuilder.singleObject(getConnection(), mapper);
    }

    public String singleString(String fieldName) {
        return queryBuilder.singleString(getConnection(), fieldName);
    }

    public Number singleLong(String fieldName) {
        return queryBuilder.singleLong(getConnection(), fieldName);
    }

    public Instant singleDateTime(String fieldName) {
        return queryBuilder.singleDateTime(getConnection(), fieldName);
    }

    public DbContextUpdateBuilder update() {
        return new DbContextUpdateBuilder(this.dbTableContext, queryBuilder.update());
    }

}
