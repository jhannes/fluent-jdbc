package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;

public class AbstractDatabaseTest {

    protected static String preprocessCreateTable(Connection connection, String createTableStatement) throws SQLException {
        String productName = connection.getMetaData().getDatabaseProductName();
        if (productName.equals("SQLite")) {
            return createTableStatement.replaceAll("auto_increment", "autoincrement");
        } else if (productName.equals("PostgreSQL")) {
            return createTableStatement
                    .replaceAll("integer primary key auto_increment", "serial primary key")
                    .replaceAll("datetime", "timestamp");
        } else {
            return createTableStatement;
        }
    }

}
