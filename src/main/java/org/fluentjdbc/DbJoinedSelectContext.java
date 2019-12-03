package org.fluentjdbc;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public class DbJoinedSelectContext implements DbListableSelectContext<DbJoinedSelectContext> {
    private final DbContext dbContext;
    private DatabaseJoinedQueryBuilder builder;

    public DbJoinedSelectContext(DbTableAliasContext dbTableContext) {
        dbContext = dbTableContext.getDbContext();
        builder = new DatabaseJoinedQueryBuilder(dbTableContext.getTableAlias());
    }

    public DbJoinedSelectContext join(DatabaseColumnReference a, DatabaseColumnReference b) {
        builder.join(a, b);
        return this;
    }

    @Override
    public <OBJECT> List<OBJECT> list(DatabaseTable.RowMapper<OBJECT> mapper) {
        return builder.list(dbContext.getThreadConnection(), mapper);
    }

    @Override
    public DbJoinedSelectContext whereExpression(String expression, @Nullable Object value) {
        builder.whereExpression(expression, value);
        return this;
    }

    public DbJoinedSelectContext whereExpressionWithMultipleParameters(String expression, Collection<?> parameters){
        builder.whereExpressionWithMultipleParameters(expression, parameters);
        return this;
    }

    @Override
    public DbJoinedSelectContext whereExpression(String expression) {
        builder.whereExpression(expression);
        return this;
    }

    @Override
    public DbJoinedSelectContext whereOptional(String fieldName, @Nullable Object value) {
        builder.whereOptional(fieldName, value);
        return this;
    }

    @Override
    public DbJoinedSelectContext whereIn(String fieldName, Collection<?> parameters) {
        builder.whereIn(fieldName, parameters);
        return this;
    }

    @Override
    public <OBJECT> OBJECT singleObject(DatabaseTable.RowMapper<OBJECT> mapper) {
        return builder.singleObject(dbContext.getThreadConnection(), mapper);
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
}

