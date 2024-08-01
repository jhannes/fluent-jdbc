package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

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
    protected UUID insert(@Nonnull Connection connection) {
        return insertWithId(
                this.idValue != null ? this.idValue : UUID.randomUUID(),
                connection
        );
    }

    @Override
    @CheckReturnValue
    protected UUID getId(DatabaseRow row) throws SQLException {
        return row.getUUID(idField);
    }

}
