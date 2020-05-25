package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface DbListableSelectContext<T extends DbListableSelectContext<T>> extends DatabaseQueriable<T> {

    <OBJECT> List<OBJECT> list(RowMapper<OBJECT> object);

    int getCount();

    default List<String> listStrings(String fieldName) {
        return list(row -> row.getString(fieldName));
    }

    default List<Long> listLongs(String fieldName) {
        return list(row -> row.getLong(fieldName));
    }

    @Nonnull
    <OBJECT> Optional<OBJECT> singleObject(RowMapper<OBJECT> mapper);

    @Nonnull
    default Optional<String> singleString(String fieldName) {
        return singleObject(row -> row.getString(fieldName));
    }

    @Nonnull
    default Optional<Number> singleLong(String fieldName) {
        return singleObject(row -> row.getLong(fieldName));
    }

    @Nonnull
    default Optional<Instant> singleInstant(String fieldName) {
        return singleObject(row -> row.getInstant(fieldName));
    }

    void forEach(DatabaseTable.RowConsumer row);

}
