package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface DatabaseTable {

    public interface RowMapper<T> {
        T mapRow(DatabaseRow row) throws SQLException;
    }


    String getTableName();

    DatabaseSaveBuilder newSaveBuilder(String idColumn, @Nullable Long idValue);

    /**
     * Use instead of {@link #newSaveBuilder} if the database driver does not
     * support RETURN_GENERATED_KEYS
     */
    DatabaseSaveBuilder newSaveBuilderNoGeneratedKeys(String idColumn, @Nullable Long idValue);

    DatabaseSaveBuilderWithUUID newSaveBuilderWithUUID(String string, @Nullable UUID uuid);


    <T> List<T> listObjects(Connection connection, RowMapper<T> mapper);

    DatabaseQueryBuilder where(String fieldName, @Nullable Object value);

    DatabaseQueryBuilder whereExpression(String expression, Object parameter);

    DatabaseQueryBuilder whereAll(List<String> uniqueKeyFields, List<Object> uniqueKeyValues);


    DatabaseInsertBuilder insert();

    DatabaseUpdateBuilder update();

}
