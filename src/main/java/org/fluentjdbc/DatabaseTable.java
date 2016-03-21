package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface DatabaseTable {

    public interface RowMapper<T> {
        T mapRow(Row row) throws SQLException;
    }

    DatabaseSaveBuilder newSaveBuilder(String idColumn, Long idValue);

    DatabaseQueryBuilder where(String fieldName, Object value);

    <T> List<T> listObjects(Connection connection, RowMapper<T> mapper);

    DatabaseQueryBuilder whereExpression(String expression, Object parameter);

}
