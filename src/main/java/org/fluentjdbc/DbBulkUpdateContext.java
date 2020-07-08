package org.fluentjdbc;

import java.util.function.Function;

public class DbBulkUpdateContext<T> implements
        DatabaseBulkQueryable<T, DbBulkUpdateContext<T>>, DatabaseBulkUpdatable<T, DbBulkUpdateContext<T>> {

    private final DbTableContext tableContext;
    private final DatabaseBulkUpdateBuilder<T> builder;

    public DbBulkUpdateContext(DbTableContext tableContext, DatabaseBulkUpdateBuilder<T> builder) {
        this.tableContext = tableContext;
        this.builder = builder;
    }

    @Override
    public DbBulkUpdateContext<T> where(String field, Function<T, ?> value) {
        return build(builder.where(field, value));
    }

    @Override
    public DbBulkUpdateContext<T> setField(String fieldName, Function<T, Object> transformer) {
        return build(builder.setField(fieldName, transformer));
    }

    private DbBulkUpdateContext<T> build(DatabaseBulkUpdateBuilder<T> builder) {
        return this;
    }

    public int execute() {
        return builder.execute(tableContext.getConnection());
    }
}
