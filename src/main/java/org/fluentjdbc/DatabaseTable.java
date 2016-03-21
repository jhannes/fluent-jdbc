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

    DatabaseSaveBuilder newSaveBuilder(String idColumn, @Nullable Long idValue);

    DatabaseQueryBuilder where(String fieldName, @Nullable Object value);

    <T> List<T> listObjects(Connection connection, RowMapper<T> mapper);

    DatabaseQueryBuilder whereExpression(String expression, Object parameter);

}
