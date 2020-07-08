package org.fluentjdbc;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbTableContext implements DatabaseQueriable<DbSelectContext> {

    private DatabaseTable table;
    private DbContext dbContext;

    public DbTableContext(DatabaseTable databaseTable, DbContext dbContext) {
        this.table = databaseTable;
        this.dbContext = dbContext;
    }

    public DbInsertContext insert() {
        return new DbInsertContext(this);
    }

    public DatabaseTable getTable() {
        return table;
    }

    public DbSelectContext whereIn(String fieldName, Collection<?> parameters) {
        return new DbSelectContext(this).whereIn(fieldName, parameters);
    }

    public DbSelectContext whereOptional(String fieldName, Object value) {
        return new DbSelectContext(this).whereOptional(fieldName, value);
    }

    public DbSelectContext whereExpression(String expression) {
        return new DbSelectContext(this).whereExpression(expression);
    }

    public DbSelectContext whereExpression(String expression, @Nullable Object value) {
        return new DbSelectContext(this).whereExpression(expression, value);
    }

    public DbSelectContext whereExpressionWithMultipleParameters(String expression, Collection<?> parameters){
        return new DbSelectContext(this).whereExpressionWithMultipleParameters(expression, parameters);
    }

    @Override
    public DbSelectContext whereAll(List<String> fields, List<Object> values) {
        return new DbSelectContext(this).whereAll(fields, values);
    }

    public DbSelectContext unordered() {
        return new DbSelectContext(this);
    }

    public DbSelectContext orderedBy(String orderByClause) {
        return new DbSelectContext(this).orderBy(orderByClause);
    }

    public Connection getConnection() {
        return dbContext.getThreadConnection();
    }

    public DbSaveBuilderContext<Long> newSaveBuilder(String idColumn, Long idValue) {
        return save(table.newSaveBuilder(idColumn, idValue));
    }

    public DbSaveBuilderContext<String> newSaveBuilderWithString(String idColumn, String idValue) {
        return save(table.newSaveBuilderWithString(idColumn, idValue));
    }

    public DbSaveBuilderContext<UUID> newSaveBuilderWithUUID(String field, UUID uuid) {
        return save(table.newSaveBuilderWithUUID(field, uuid));
    }

    public <T> DbSaveBuilderContext<T> save(DatabaseSaveBuilder<T> saveBuilder) {
        return new DbSaveBuilderContext<>(this, saveBuilder);
    }

    public <KEY,ENTITY> Optional<ENTITY> cache(KEY key, RetrieveMethod<KEY, ENTITY> retriever) {
        return dbContext.cache(getTable().getTableName(), key, retriever);
    }

    public DbTableAliasContext alias(String alias) {
        return new DbTableAliasContext(this, alias);
    }

    public DbContext getDbContext() {
        return dbContext;
    }

    @Override
    public DbSelectContext query() {
        return new DbSelectContext(this);
    }

    public <T> DbSyncBuilderContext<T> synch(List<T> entities) {
        return new DbSyncBuilderContext<>(this, entities);
    }

    public <T> DbBulkInsertContext<T> bulkInsert(Stream<T> objects) {
        return bulkInsert(objects.collect(Collectors.toList()));
    }

    public <T> DbBulkInsertContext<T> bulkInsert(Iterable<T> objects) {
        return new DbBulkInsertContext<>(this, table.bulkInsert(objects));
    }

    public <T> DbBulkDeleteContext<T> bulkDelete(Stream<T> objects) {
        return bulkDelete(objects.collect(Collectors.toList()));
    }

    public <T> DbBulkDeleteContext<T> bulkDelete(Iterable<T> objects) {
        return new DbBulkDeleteContext<>(this, table.bulkDelete(objects));
    }

    public <T> DbBulkUpdateContext<T> bulkUpdate(Stream<T> objects) {
        return bulkUpdate(objects.collect(Collectors.toList()));
    }

    private <T> DbBulkUpdateContext<T> bulkUpdate(List<T> objects) {
        return new DbBulkUpdateContext<>(this, table.bulkUpdate(objects));
    }
}
