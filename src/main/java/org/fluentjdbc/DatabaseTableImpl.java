package org.fluentjdbc;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;

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
    public DatabaseSaveBuilder<Long> newSaveBuilder(String idField, @Nullable Long id) {
        return new DatabaseSaveBuilderWithLong(this, idField, id);
    }

    @Override
    public DatabaseSaveBuilder<Long> newSaveBuilderNoGeneratedKeys(String idField, @Nullable Long id) {
        return new DatabaseSaveBuilderWithoutGeneratedKeys(this, idField, id);
    }

    @Override
    public DatabaseSaveBuilderWithUUID newSaveBuilderWithUUID(String idField, @Nullable UUID id) {
        return new DatabaseSaveBuilderWithUUID(this, idField, id);
    }

    @Override
    public DatabaseSimpleQueryBuilder where(String fieldName, @Nullable Object value) {
        return new DatabaseQueryBuilder(this).where(fieldName, value);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereOptional(String fieldName, @Nullable Object value) {
        return new DatabaseQueryBuilder(this).whereOptional(fieldName, value);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereIn(String fieldName, List<?> parameters) {
        return new DatabaseQueryBuilder(this).whereIn(fieldName, parameters);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression) {
        return new DatabaseQueryBuilder(this).whereExpression(expression);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression, Object parameter) {
        return new DatabaseQueryBuilder(this).whereExpression(expression, parameter);
    }

    @Override
    public <T> List<T> listObjects(Connection connection, RowMapper<T> mapper) {
        return new DatabaseQueryBuilder(this).list(connection, mapper);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereAll(List<String> fieldNames, List<Object> values) {
        return new DatabaseQueryBuilder(this).whereAll(fieldNames, values);
    }

    @Override
    public DatabaseInsertBuilder insert() {
        return new DatabaseInsertBuilder(tableName);
    }

    @Override
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(List<T> objects) {
        return new DatabaseBulkInsertBuilder<T>(this, objects);
    }

    @Override
    public DatabaseUpdateBuilder update() {
        return new DatabaseUpdateBuilder(tableName);
    }

    @Override
    public DatabaseDeleteBuilder delete() {
        return new DatabaseDeleteBuilder(tableName);
    }
}