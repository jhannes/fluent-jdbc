package org.fluentjdbc;

import java.sql.Connection;
import java.time.Instant;

import javax.annotation.Nullable;

import org.fluentjdbc.DatabaseTable.RowMapper;

public interface DatabaseSimpleQueryBuilder {

    String singleString(Connection connection, String string);

    <T> T singleObject(Connection connection, RowMapper<T> mapper);

    Instant singleDateTime(Connection connection, String fieldName);

    DatabaseUpdateBuilder update();

    void delete(Connection connection);

    Number singleLong(Connection connection, String fieldName);

    DatabaseListableQueryBuilder unordered();

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    DatabaseSimpleQueryBuilder where(String fieldName, @Nullable Object value);

    DatabaseSimpleQueryBuilder whereOptional(String fieldName, @Nullable Object value);


}
