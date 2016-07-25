package org.fluentjdbc.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class H2TestDatabase {

    public static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("UUID", "uuid");
        REPLACEMENTS.put("INTEGER_PK", "integer primary key auto_increment");
        REPLACEMENTS.put("DATETIME", "datetime");
    }

    public static Connection createConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:FluentJdbcDemoTest");
        return DriverManager.getConnection(jdbcUrl);
    }

}
