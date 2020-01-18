package org.fluentjdbc;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class DbBuildInsertContext<T> {

    private final DbTableContext tableContext;
    private DatabaseBulkInsertBuilder<T> builder;

    public DbBuildInsertContext(DbTableContext tableContext, Iterable<T> objects) {
        builder = tableContext.getTable().bulkInsert(objects);
        this.tableContext = tableContext;
    }

    public DbBuildInsertContext(DbTableContext tableContext, Stream<T> objects) {
        builder = tableContext.getTable().bulkInsert(objects);
        this.tableContext = tableContext;
    }

    public <VALUES extends List<?>> DbBuildInsertContext<T> setFields(List<String> fields, Function<T, VALUES> values) {
        return build(builder.setFields(fields, values));
    }

    private DbBuildInsertContext<T> build(DatabaseBulkInsertBuilder<T> builder) {
        return this;
    }

    public void execute() {
        builder.execute(tableContext.getConnection());
    }
}
