package org.fluentjdbc;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

import org.fluentjdbc.DatabaseTable.RowMapper;

public interface DatabaseListableQueryBuilder {

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    <T> Stream<T> stream(Connection connection, RowMapper<T> mapper);

    <T> List<T> list(Connection connection, RowMapper<T> mapper);

    int getCount(Connection connection);

    default List<Long> listLongs(Connection connection, final String fieldName) {
        return list(connection, row -> row.getLong(fieldName));
    }

    default List<String> listStrings(Connection connection, final String fieldName) {
        return list(connection, row -> row.getString(fieldName));
    }

}
