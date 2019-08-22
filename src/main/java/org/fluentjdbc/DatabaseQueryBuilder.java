package org.fluentjdbc;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.time.Instant;

public interface DatabaseQueryBuilder<T extends DatabaseQueryBuilder<T>> extends DatabaseQueriable<T> {

    DatabaseListableQueryBuilder unordered();

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    <OBJECT> OBJECT singleObject(Connection connection, DatabaseTable.RowMapper<OBJECT> mapper);

    @Nullable
    default String singleString(Connection connection, String fieldName) {
        return singleObject(connection, row -> row.getString(fieldName));
    }

    @Nullable
    default Number singleLong(Connection connection, final String fieldName) {
        return singleObject(connection, (DatabaseTable.RowMapper<Number>) row -> row.getLong(fieldName));
    }

    @Nullable
    default Instant singleInstant(Connection connection, final String fieldName) {
        return singleObject(connection, row -> row.getInstant(fieldName));
    }

}
