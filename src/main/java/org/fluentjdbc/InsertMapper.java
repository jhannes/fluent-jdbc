package org.fluentjdbc;

import java.sql.SQLException;

public interface InsertMapper<T> {

    void mapRow(Inserter inserter, T o) throws SQLException;

}
