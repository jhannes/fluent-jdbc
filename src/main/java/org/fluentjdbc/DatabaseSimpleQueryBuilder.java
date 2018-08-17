package org.fluentjdbc;

import java.sql.Connection;

import javax.annotation.Nullable;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.joda.time.DateTime;

public interface DatabaseSimpleQueryBuilder {

    String singleString(Connection connection, String string);

    <T> T singleObject(Connection connection, RowMapper<T> mapper);

    DateTime singleDateTime(Connection connection, String fieldName);

    DatabaseUpdateBuilder update();

    Number singleLong(Connection connection, String fieldName);

    DatabaseListableQueryBuilder unordered();

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    DatabaseSimpleQueryBuilder where(String fieldName, @Nullable Object value);

    DatabaseSimpleQueryBuilder whereOptional(String fieldName, @Nullable Object value);

}
