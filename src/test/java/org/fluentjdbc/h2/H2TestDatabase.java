package org.fluentjdbc.h2;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@RunWith(Enclosed.class)
public class H2TestDatabase {

    public static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("INT_ARRAY", "Integer Array");
        REPLACEMENTS.put("STRING_ARRAY", "Varchar Array");
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
