package org.fluentjdbc;

import java.sql.Connection;
import java.time.Instant;

import org.fluentjdbc.DatabaseTable.RowMapper;

public interface DatabaseSimpleQueryBuilder extends DatabaseQueriable<DatabaseSimpleQueryBuilder> {

    DatabaseListableQueryBuilder unordered();

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    <T> T singleObject(Connection connection, RowMapper<T> mapper);

    String singleString(Connection connection, String string);

    Instant singleInstant(Connection connection, String fieldName);

    Number singleLong(Connection connection, String fieldName);

    DatabaseUpdateBuilder update();

    void delete(Connection connection);
}
