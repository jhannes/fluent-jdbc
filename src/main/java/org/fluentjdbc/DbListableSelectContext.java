package org.fluentjdbc;

import java.util.List;

import org.fluentjdbc.DatabaseTable.RowMapper;

public interface DbListableSelectContext<T extends DbListableSelectContext<T>> extends DatabaseQueriable<T> {

    <OBJECT> List<OBJECT> list(RowMapper<OBJECT> object);

    default List<String> listStrings(String fieldName) {
        return list(row -> row.getString(fieldName));
    }

    default List<Long> listLongs(String fieldName) {
        return list(row -> row.getLong(fieldName));
    }
}
