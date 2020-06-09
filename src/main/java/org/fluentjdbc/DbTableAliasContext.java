package org.fluentjdbc;

import javax.annotation.Nullable;
import java.util.Map;

public class DbTableAliasContext  {
    private final DbTableContext dbTableContext;
    private final DatabaseTableAlias alias;

    public DbTableAliasContext(DbTableContext dbTableContext, String alias) {
        this.dbTableContext = dbTableContext;
        this.alias = dbTableContext.getTable().alias(alias);
    }

    public DatabaseColumnReference column(String columnName) {
        return alias.column(columnName);
    }

    public DbJoinedSelectContext join(DatabaseColumnReference a, DatabaseColumnReference b) {
        return select().join(a, b);
    }

    public DbJoinedSelectContext leftJoin(DatabaseColumnReference a, DatabaseColumnReference b) {
        return select().leftJoin(a, b);
    }

    public DatabaseTableAlias getTableAlias() {
        return alias;
    }

    public DbJoinedSelectContext where(String fieldName, @Nullable Object value) {
        return select().whereExpression(alias.getAlias() + "." + fieldName + " = ?", value);
    }

    public DbJoinedSelectContext select() {
        return new DbJoinedSelectContext(this);
    }

    public DbContext getDbContext() {
        return dbTableContext.getDbContext();
    }

}
