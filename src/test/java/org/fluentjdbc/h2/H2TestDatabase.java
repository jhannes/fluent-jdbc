package org.fluentjdbc.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class H2TestDatabase {

    public static Connection createConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:FluentJdbcDemoTest");
        return DriverManager.getConnection(jdbcUrl);
    }

}
