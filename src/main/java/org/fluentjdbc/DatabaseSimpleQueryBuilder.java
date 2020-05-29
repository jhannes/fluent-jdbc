package org.fluentjdbc;

import java.sql.Connection;

public interface DatabaseSimpleQueryBuilder extends DatabaseQueryBuilder<DatabaseSimpleQueryBuilder> {

    DatabaseUpdateBuilder update();

    int delete(Connection connection);

}
