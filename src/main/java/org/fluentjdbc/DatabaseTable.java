package org.fluentjdbc;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

@ParametersAreNonnullByDefault
public interface DatabaseTable extends DatabaseQueriable<DatabaseSimpleQueryBuilder> {

    DatabaseListableQueryBuilder unordered();

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    DatabaseTableAlias alias(String alias);

    @FunctionalInterface
    interface RowMapper<T> {
        T mapRow(DatabaseRow row) throws SQLException;
    }

    @FunctionalInterface
    interface RowConsumer {
        void apply(DatabaseRow row) throws SQLException;
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

    <T> DatabaseBulkInsertBuilder<T> bulkInsert(Iterable<T> objects);

    <T> DatabaseBulkInsertBuilder<T> bulkInsert(Stream<T> objects);

    <T> DatabaseBulkDeleteBuilder<T> bulkDelete(Iterable<T> objects);

    <T> DatabaseBulkUpdateBuilder<T> bulkUpdate(List<T> objects);

    DatabaseDeleteBuilder delete();

}
