package org.fluentjdbc;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        return query().orderBy(orderByClause);
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
        return query().whereOptional(fieldName, value);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression) {
        return query().whereExpression(expression);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression, Object parameter) {
        return query().whereExpression(expression, parameter);
    }

    public DatabaseSimpleQueryBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters){
        return query().whereExpressionWithMultipleParameters(expression, parameters);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereAll(List<String> fields, List<Object> values) {
        return query().whereAll(fields, values);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        return query().whereIn(fieldName, parameters);
    }

    @Override
    public DatabaseSimpleQueryBuilder query() {
        return new DatabaseTableQueryBuilder(this);
    }

    @Override
    public DatabaseInsertBuilder insert() {
        return new DatabaseInsertBuilder(tableName);
    }

    @Override
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(Iterable<T> objects) {
        return new DatabaseBulkInsertBuilder<>(this, objects);
    }

    @Override
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(Stream<T> objects) {
        return bulkInsert(objects.collect(Collectors.toList()));
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
