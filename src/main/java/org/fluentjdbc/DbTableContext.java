package org.fluentjdbc;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.fluentjdbc.DatabaseTable.RowMapper;

import javax.annotation.Nullable;

public class DbTableContext implements DatabaseQueriable<DbSelectContext> {

    private DatabaseTable table;
    private DbContext dbContext;

    public DbTableContext(String tableName, DbContext dbContext) {
        this(new DatabaseTableImpl(tableName), dbContext);
    }

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
        return new DbSaveBuilderContext<>(this, table.newSaveBuilder(idColumn, idValue));
    }

    public DbSaveBuilderContext<UUID> newSaveBuilderWithUUID(String field, UUID uuid) {
        return new DbSaveBuilderContext<>(this, table.newSaveBuilderWithUUID(field, uuid));
    }

    public <KEY,ENTITY> ENTITY cache(KEY key, RetrieveMethod<KEY, ENTITY> retriever) {
        return DbContext.cache(getTable().getTableName(), key, retriever);
    }

    public DbTableAliasContext alias(String alias) {
        return new DbTableAliasContext(this, alias);
    }

    public DbContext getDbContext() {
        return dbContext;
    }
}
