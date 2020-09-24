package org.fluentjdbc.h2;

import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class H2TestDatabase {

    public static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("UUID", "uuid");
        REPLACEMENTS.put("INTEGER_PK", "serial primary key");
        REPLACEMENTS.put("DATETIME", "datetime");
        REPLACEMENTS.put("BOOLEAN", "boolean");
        REPLACEMENTS.put("INT_ARRAY", "Array");
        REPLACEMENTS.put("STRING_ARRAY", "Array");
    }

    public static Connection createConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:FluentJdbcDemoTest");
        return DriverManager.getConnection(jdbcUrl);
    }

    public static DataSource createDataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:FluentJdbcDemoTest;DB_CLOSE_DELAY=-1");
        return dataSource;
    }

}
