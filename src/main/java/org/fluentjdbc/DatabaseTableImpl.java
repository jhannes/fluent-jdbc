package org.fluentjdbc;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

public class DatabaseTableImpl implements DatabaseTable {

    private final String tableName;

    public DatabaseTableImpl(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public DatabaseListableQueryBuilder unordered() {
        return new DatabaseTableQueryBuilder(this);
    }

    @Override
    public DatabaseListableQueryBuilder orderBy(String orderByClause) {
        return new DatabaseTableQueryBuilder(this).orderBy(orderByClause);
    }

    @Override
    public DatabaseTableAlias alias(String alias) {
        return new DatabaseTableAlias(this, alias);
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
    public DatabaseSimpleQueryBuilder whereOptional(String fieldName, @Nullable Object value) {
        return new DatabaseTableQueryBuilder(this).whereOptional(fieldName, value);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression) {
        return new DatabaseTableQueryBuilder(this).whereExpression(expression);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression, Object parameter) {
        return new DatabaseTableQueryBuilder(this).whereExpression(expression, parameter);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereAll(List<String> fieldNames, List<Object> values) {
        return new DatabaseTableQueryBuilder(this).whereAll(fieldNames, values);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        return new DatabaseTableQueryBuilder(this).whereIn(fieldName, parameters);
    }

    @Override
    public DatabaseInsertBuilder insert() {
        return new DatabaseInsertBuilder(tableName);
    }

    @Override
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(List<T> objects) {
        return new DatabaseBulkInsertBuilder<>(this, objects);
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