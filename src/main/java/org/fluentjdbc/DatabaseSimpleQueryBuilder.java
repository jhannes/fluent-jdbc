package org.fluentjdbc;

import java.sql.Connection;
import java.time.Instant;

import org.fluentjdbc.DatabaseTable.RowMapper;

import javax.annotation.Nullable;

public interface DatabaseSimpleQueryBuilder extends DatabaseQueriable<DatabaseSimpleQueryBuilder> {

    DatabaseListableQueryBuilder unordered();

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    DatabaseUpdateBuilder update();

    void delete(Connection connection);

    <T> T singleObject(Connection connection, RowMapper<T> mapper);

    @Nullable
    default String singleString(Connection connection, String fieldName) {
        return singleObject(connection, row -> row.getString(fieldName));
    }

    @Nullable
    default Number singleLong(Connection connection, final String fieldName) {
        return singleObject(connection, (RowMapper<Number>) row -> row.getLong(fieldName));
    }

    @Nullable
    default Instant singleInstant(Connection connection, final String fieldName) {
        return singleObject(connection, row -> row.getInstant(fieldName));
    }

}
