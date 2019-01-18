package org.fluentjdbc;

public class DbContextUpdateBuilder {

    private DbTableContext tableContext;
    private DatabaseUpdateBuilder updateBuilder;

    public DbContextUpdateBuilder(DbTableContext tableContext, DatabaseUpdateBuilder updateBuilder) {
        this.tableContext = tableContext;
        this.updateBuilder = updateBuilder;
    }

    public DbContextUpdateBuilder setField(String field, Object value) {
        updateBuilder.setField(field, value);
        return this;
    }

    public DbContextUpdateBuilder setFieldIfPresent(String field, Object value) {
        updateBuilder.setFieldIfPresent(field, value);
        return this;
    }

    public void execute() {
        updateBuilder.execute(tableContext.getConnection());
    }


}
