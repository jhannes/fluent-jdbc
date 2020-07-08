package org.fluentjdbc;

import java.util.List;
import java.util.function.Function;

public class DbBulkInsertContext<T> implements DatabaseBulkUpdatable<T, DbBulkInsertContext<T>> {

    private final DbTableContext tableContext;
    private final DatabaseBulkInsertBuilder<T> builder;

    public DbBulkInsertContext(DbTableContext tableContext, DatabaseBulkInsertBuilder<T> builder) {
        this.tableContext = tableContext;
        this.builder = builder;
    }

    @Override
    public DbBulkInsertContext<T> setField(String fieldName, Function<T, Object> transformer) {
        return build(builder.setField(fieldName, transformer));
    }

    @Override
    public <VALUES extends List<?>> DbBulkInsertContext<T> setFields(List<String> fields, Function<T, VALUES> values) {
        return build(builder.setFields(fields, values));
    }

    private DbBulkInsertContext<T> build(DatabaseBulkInsertBuilder<T> builder) {
        return this;
    }

    public int execute() {
        return builder.execute(tableContext.getConnection());
    }
}
