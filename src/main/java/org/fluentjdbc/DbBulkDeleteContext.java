package org.fluentjdbc;

import java.util.function.Function;

public class DbBulkDeleteContext<T> implements DatabaseBulkQueryable<T, DbBulkDeleteContext<T>> {
    private final DbTableContext tableContext;
    private final DatabaseBulkDeleteBuilder<T> builder;

    public DbBulkDeleteContext(DbTableContext tableContext, DatabaseBulkDeleteBuilder<T> builder) {
        this.tableContext = tableContext;
        this.builder = builder;
    }

    @Override
    public DbBulkDeleteContext<T> where(String field, Function<T, ?> value) {
        return build(builder.where(field, value));
    }

    private DbBulkDeleteContext<T> build(DatabaseBulkDeleteBuilder<T> builder) {
        return this;
    }

    public int execute() {
        return builder.execute(tableContext.getConnection());
    }
}
