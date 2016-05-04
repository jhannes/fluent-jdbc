package org.fluentjdbc;

import java.sql.Connection;

import javax.annotation.Nullable;

public class DatabaseSaveBuilderWithoutGeneratedKeys extends DatabaseSaveBuilderWithLong {

    public DatabaseSaveBuilderWithoutGeneratedKeys(DatabaseTableImpl table, String idField, Long id) {
        super(table, idField, id);
    }

    @Override
    protected @Nullable Long insert(Connection connection) {
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
