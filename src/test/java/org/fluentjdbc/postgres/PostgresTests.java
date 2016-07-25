package org.fluentjdbc.postgres;

import org.junit.Assume;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class PostgresTests {

    private static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("INTEGER_PK", "serial primary key");
        REPLACEMENTS.put("DATETIME", "timestamp");
    }

    public static class DatabaseSaveBuilderTest extends org.fluentjdbc.DatabaseSaveBuilderTest {
        public DatabaseSaveBuilderTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    public static class RichDomainModelTest extends org.fluentjdbc.RichDomainModelTest {
        public RichDomainModelTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    public static class FluentJdbcDemonstrationTest extends org.fluentjdbc.FluentJdbcDemonstrationTest {
        public FluentJdbcDemonstrationTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    public static class DatabaseTableTest extends org.fluentjdbc.DatabaseTableTest {
        public DatabaseTableTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    public static class BulkInsertTest extends org.fluentjdbc.BulkInsertTest {
        public BulkInsertTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    static Connection getConnection() throws SQLException {
        String username = System.getProperty("test.db.postgres.username", "fluentjdbc_test");
        String password = System.getProperty("test.db.postgres.password", username);
        String url = System.getProperty("test.db.postgres.url", "jdbc:postgresql:" + username);

        try {
            return DriverManager.getConnection(url, username, password);
        } catch (PSQLException e) {
            if (e.getSQLState().equals(PSQLState.CONNECTION_UNABLE_TO_CONNECT.getState())) {
                Assume.assumeNoException(e);
            }
            throw e;
        }
    }
}
