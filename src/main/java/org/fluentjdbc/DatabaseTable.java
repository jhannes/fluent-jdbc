package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface DatabaseTable {

    public interface RowMapper<T> {
        T mapRow(DatabaseRow row) throws SQLException;
    }

    String getTableName();

    DatabaseSaveBuilder newSaveBuilder(String idColumn, @Nullable Number idValue);

    <T> List<T> listObjects(Connection connection, RowMapper<T> mapper);

    DatabaseQueryBuilder where(String fieldName, @Nullable Object value);

    DatabaseQueryBuilder whereExpression(String expression, Object parameter);

    DatabaseQueryBuilder whereAll(List<String> uniqueKeyFields, List<Object> uniqueKeyValues);

    DatabaseInsertBuilder insert();

}
