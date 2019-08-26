package org.fluentjdbc;

import javax.annotation.Nullable;

public class DbTableAliasContext  {
    private final DbTableContext dbTableContext;
    private final DatabaseTableAlias alias;

    public DbTableAliasContext(DbTableContext dbTableContext, String alias) {
        this.dbTableContext = dbTableContext;
        this.alias = new DatabaseTableAlias(dbTableContext.getTable(), alias);
    }

    public DatabaseColumnReference column(String columnName) {
        return alias.column(columnName);
    }

    public DbJoinedSelectContext join(DatabaseColumnReference a, DatabaseColumnReference b) {
        return select().join(a, b);
    }

    public DatabaseTableAlias getTableAlias() {
        return alias;
    }

    public DbJoinedSelectContext where(String fieldName, @Nullable Object value) {
        return select().whereExpression(alias.getAlias() + "." + fieldName + " = ?", value);
    }

    protected DbJoinedSelectContext select() {
        return new DbJoinedSelectContext(this);
    }

    public DbContext getDbContext() {
        return dbTableContext.getDbContext();
    }
}
