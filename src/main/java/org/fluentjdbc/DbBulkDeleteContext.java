package org.fluentjdbc;

import java.util.List;
import java.util.function.Function;

public class DbBulkDeleteContext<T> {
    private final DbTableContext tableContext;
    private final DatabaseBulkDeleteBuilder<T> builder;

    public DbBulkDeleteContext(DbTableContext tableContext, DatabaseBulkDeleteBuilder<T> builder) {
        this.tableContext = tableContext;
        this.builder = builder;
    }

    public DbBulkDeleteContext<T> whereAll(List<String> fields, Function<T, List<?>> values) {
        return build(builder.whereAll(fields, values));
    }

    private DbBulkDeleteContext<T> build(DatabaseBulkDeleteBuilder<T> builder) {
        return this;
    }

    public int execute() {
        return builder.execute(tableContext.getConnection());
    }
}
