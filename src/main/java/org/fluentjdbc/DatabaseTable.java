package org.fluentjdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface DatabaseTable {

    public interface RowMapper<T> {
        T mapRow(ResultSet rs) throws SQLException;
    }

    DatabaseSaveBuilder newSaveBuilder(String idColumn, Long idValue);

    DatabaseQueryBuilder where(String fieldName, Object value);

    <T> List<T> listObjects(Connection connection, RowMapper<T> mapper);

}
