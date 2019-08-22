package org.fluentjdbc;

import java.sql.Connection;

public interface DatabaseSimpleQueryBuilder extends DatabaseQueryBuilder<DatabaseSimpleQueryBuilder> {

    DatabaseUpdateBuilder update();

    void delete(Connection connection);

}
