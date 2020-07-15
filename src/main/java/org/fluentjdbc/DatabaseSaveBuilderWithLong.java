package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Subclass of {@link DatabaseSaveBuilder} which uses
 * {@link DatabaseInsertBuilder#setPrimaryKey(String, Object)} in order to
 * autogenerate primary key field
 */
public class DatabaseSaveBuilderWithLong extends DatabaseSaveBuilder<Long> {

    DatabaseSaveBuilderWithLong(DatabaseTable table, String idField, Long id) {
        super(table, idField, id);
    }

    @Override
    @Nonnull
    protected Long insert(Connection connection) throws SQLException {
        return table.insert()
            .setPrimaryKey(idField, idValue)
            .setFields(fields, values)
            .setFields(uniqueKeyFields, uniqueKeyValues)
            .execute(connection);
    }

}
