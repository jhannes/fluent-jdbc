package org.fluentjdbc;

import java.sql.Connection;

import javax.annotation.Nullable;

public class DatabaseSaveBuilderWithoutGeneratedKeys<T> extends DatabaseSaveBuilder<T> {

    public DatabaseSaveBuilderWithoutGeneratedKeys(DatabaseTableImpl table, String idField, T id) {
        super(table, idField, id);
    }

    @Override
    @Nullable
    protected T insert(Connection connection) {
        DatabaseInsertBuilder builder = table.insert();
        if (idValue != null) {
            builder = builder.setField(idField, idValue);
        }
        builder
            .setFields(fields, values)
            .setFields(uniqueKeyFields, uniqueKeyValues)
            .execute(connection);
        return idValue;
    }
}
