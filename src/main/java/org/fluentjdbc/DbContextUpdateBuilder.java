package org.fluentjdbc;

import java.util.Collection;

public class DbContextUpdateBuilder implements DatabaseUpdateable<DbContextUpdateBuilder> {

    private DbTableContext tableContext;
    private DatabaseUpdateBuilder updateBuilder;

    public DbContextUpdateBuilder(DbTableContext tableContext, DatabaseUpdateBuilder updateBuilder) {
        this.tableContext = tableContext;
        this.updateBuilder = updateBuilder;
    }

    @Override
    public DbContextUpdateBuilder setFields(Collection<String> fields, Collection<Object> values) {
        updateBuilder.setFields(fields, values);
        return this;
    }

    @Override
    public DbContextUpdateBuilder setField(String field, Object value) {
        updateBuilder.setField(field, value);
        return this;
    }

    @Override
    public DbContextUpdateBuilder setFieldIfPresent(String field, Object value) {
        updateBuilder.setFieldIfPresent(field, value);
        return this;
    }

    public void execute() {
        updateBuilder.execute(tableContext.getConnection());
    }


}
