package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface DatabaseTable extends DatabaseQueriable<DatabaseSimpleQueryBuilder> {

    DatabaseListableQueryBuilder unordered();

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    DatabaseTableAlias alias(String alias);

    @FunctionalInterface
    interface RowMapper<T> {
        T mapRow(DatabaseRow row) throws SQLException;
    }

    String getTableName();

    DatabaseSaveBuilder<Long> newSaveBuilder(String idColumn, @Nullable Long idValue);

    /**
     * Use instead of {@link #newSaveBuilder} if the database driver does not
     * support RETURN_GENERATED_KEYS
     */
    DatabaseSaveBuilder<Long> newSaveBuilderNoGeneratedKeys(String idColumn, @Nullable Long idValue);

    DatabaseSaveBuilder<UUID> newSaveBuilderWithUUID(String fieldName, @Nullable UUID uuid);

    DatabaseInsertBuilder insert();

    DatabaseUpdateBuilder update();

    <T> DatabaseBulkInsertBuilder<T> bulkInsert(List<T> objects);

    DatabaseDeleteBuilder delete();

}
