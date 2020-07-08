package org.fluentjdbc;

import java.sql.SQLException;

import org.fluentjdbc.util.ExceptionUtil;

public class DbSaveBuilderContext<T> {

    private final DbTableContext tableContext;
    private final DatabaseSaveBuilder<T> saveBuilder;

    public DbSaveBuilderContext(DbTableContext tableContext, DatabaseSaveBuilder<T> saveBuilder) {
        this.tableContext = tableContext;
        this.saveBuilder = saveBuilder;
    }

    public DbSaveBuilderContext<T> uniqueKey(String fieldName, Object fieldValue) {
        saveBuilder.uniqueKey(fieldName, fieldValue);
        return this;
    }

    public DbSaveBuilderContext<T> setField(String fieldName, Object fieldValue) {
        saveBuilder.setField(fieldName, fieldValue);
        return this;
    }

    public DatabaseSaveResult<T> execute() {
        try {
            return saveBuilder.execute(tableContext.getConnection());
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

}
