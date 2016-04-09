package org.fluentjdbc;

import java.sql.Connection;
import java.util.List;

import javax.annotation.Nullable;

public class DatabaseTableImpl implements DatabaseTable {

    private final String tableName;

    public DatabaseTableImpl(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public DatabaseSaveBuilder newSaveBuilder(String idField, @Nullable Long id) {
        return new DatabaseSaveBuilder(this, idField, id);
    }

    @Override
    public DatabaseSaveBuilder newSaveBuilderNoGeneratedKeys(String idField, @Nullable Long id) {
        return new DatabaseSaveBuilderWithoutGeneratedKeys(this, idField, id);
    }

    @Override
    public DatabaseQueryBuilder where(String fieldName, @Nullable Object value) {
        return new DatabaseQueryBuilder(this).where(fieldName, value);
    }

    @Override
    public DatabaseQueryBuilder whereExpression(String expression, Object parameter) {
        return new DatabaseQueryBuilder(this).whereExpression(expression, parameter);
    }

    @Override
    public <T> List<T> listObjects(Connection connection, RowMapper<T> mapper) {
        return new DatabaseQueryBuilder(this).list(connection, mapper);
    }

    @Override
    public DatabaseQueryBuilder whereAll(List<String> fieldNames, List<Object> values) {
        return new DatabaseQueryBuilder(this).whereAll(fieldNames, values);
    }

    @Override
    public DatabaseInsertBuilder insert() {
        return new DatabaseInsertBuilder(this);
    }

    @Override
    public DatabaseUpdateBuilder update() {
        return new DatabaseUpdateBuilder(this);
    }

}