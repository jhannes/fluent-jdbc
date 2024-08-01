package org.fluentjdbc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;

/**
 * Subclass of {@link DatabaseSaveBuilder} which assumes the client code always specifies
 * primary key field
 */
public class DatabaseSaveBuilderWithoutGeneratedKeys<T> extends DatabaseSaveBuilder<T> {

    public DatabaseSaveBuilderWithoutGeneratedKeys(DatabaseTableImpl table, String idField, T id) {
        super(table, idField, id);
    }

    @Override
    @Nullable
    protected T insert(@Nonnull Connection connection) {
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
