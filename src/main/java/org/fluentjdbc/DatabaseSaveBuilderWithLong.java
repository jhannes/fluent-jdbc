package org.fluentjdbc;

import java.sql.Connection;

public class DatabaseSaveBuilderWithLong extends DatabaseSaveBuilder<Long> {

    DatabaseSaveBuilderWithLong(DatabaseTable table, String idField, Long id) {
        super(table, idField, id);
    }

    @Override
    protected Long insert(Connection connection) {
        return table.insert()
            .setPrimaryKey(idField, idValue)
            .setFields(fields, values)
            .setFields(uniqueKeyFields, uniqueKeyValues)
            .execute(connection);
    }

}
