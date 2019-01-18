package org.fluentjdbc;

import java.util.List;

import org.fluentjdbc.DatabaseTable.RowMapper;

public interface DbListableSelectContext {

    List<String> listStrings(String fieldName);

    <T> List<T> list(RowMapper<T> object);

}
