package org.fluentjdbc;

import java.sql.Connection;
import java.util.List;

import org.fluentjdbc.DatabaseTable.RowMapper;

public interface DatabaseListableQueryBuilder {

    <T> List<T> list(Connection connection, RowMapper<T> mapper);

    List<Long> listLongs(Connection connection, String fieldName);

    List<String> listStrings(Connection connection, String fieldName);

}
