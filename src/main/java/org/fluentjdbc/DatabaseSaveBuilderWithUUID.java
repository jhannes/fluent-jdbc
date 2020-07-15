package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Subclass of {@link DatabaseSaveBuilder} which uses {@link UUID#randomUUID()} to generate primary
 * key values
 */
@ParametersAreNonnullByDefault
public class DatabaseSaveBuilderWithUUID extends DatabaseSaveBuilder<UUID> {


    public DatabaseSaveBuilderWithUUID(DatabaseTableImpl table, String idField, @Nullable UUID idValue) {
        super(table, idField, idValue);
    }

    @Override
    @Nullable
    protected UUID insert(Connection connection) {
        UUID idValue = this.idValue;
        if (idValue == null) {
            idValue = UUID.randomUUID();
        }
        table.insert()
                .setFields(fields, values)
                .setField(idField, idValue)
                .setFields(uniqueKeyFields, uniqueKeyValues)
                .execute(connection);
        return idValue;
    }

    @Override
    protected UUID getId(DatabaseRow row) throws SQLException {
        return row.getUUID(idField);
    }

}
