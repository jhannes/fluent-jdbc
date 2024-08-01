package org.fluentjdbc.sqlserver;

import org.junit.Assume;
import org.junit.Ignore;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@RunWith(Enclosed.class)
public class JtdsSqlServerTests {

    private static final Map<String, String> REPLACEMENTS = new HashMap<>();

    static {
        REPLACEMENTS.put("UUID", "uniqueidentifier");
        REPLACEMENTS.put("INTEGER_PK", "integer identity primary key");
        REPLACEMENTS.put("BOOLEAN", "bit");
    }

    public static class DatabaseSaveBuilderTest extends org.fluentjdbc.DatabaseSaveBuilderTest {
        public DatabaseSaveBuilderTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    public static class RichDomainModelTest extends org.fluentjdbc.RichDomainModelTest {
        public RichDomainModelTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
            databaseDoesNotSupportResultSetMetadataTableName();
        }

        @Override
        @Ignore
        public void shouldBulkInsert() {
            //super.shouldBulkInsert();
            // Ignore - relies on the combination of addBatch and RETURN_GENERATED_KEYS
            // Which is not supported by jTDS
        }
    }

    public static class FluentJdbcDemonstrationTest extends org.fluentjdbc.FluentJdbcDemonstrationTest {
        public FluentJdbcDemonstrationTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }

        @Override
        public void shouldInsertRowWithNonexistentKey() throws SQLException {
            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("set identity_insert demo_table on");
            }

            super.shouldInsertRowWithNonexistentKey();

            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("set identity_insert demo_table off");
            }
        }
    }

    public static class DatabaseTableTest extends org.fluentjdbc.DatabaseTableTest {
        public DatabaseTableTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }

        @Override
        public void shouldInsertWithExplicitKey() throws SQLException {
            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("set identity_insert database_table_test_table on");
            }

            super.shouldInsertWithExplicitKey();

            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("set identity_insert database_table_test_table off");
            }
        }
    }

    public static class BulkInsertTest extends org.fluentjdbc.BulkInsertTest {
        public BulkInsertTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    private static boolean databaseDisabled = Boolean.getBoolean("test.db.jtds.disabled");

    static Connection getConnection() throws SQLException {
        Assume.assumeFalse(databaseDisabled);
        String username = System.getProperty("test.db.sqlserver.username", "sa");
        String password = System.getProperty("test.db.sqlserver.password", "0_A_SECRET_p455w0rd");
        String url = System.getProperty("test.db.sqlserver.url",
                "jdbc:jtds:sqlserver://localhost:1433;loginTimeout=" + 2);

        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            if (e.getSQLState().equals("08S03") || e.getSQLState().equals("08S01") || e.getSQLState().equals("HYT01")) {
                databaseDisabled = true;
                Assume.assumeFalse("Database is unavailable: " + e, true);
            }
            throw e;
        }
    }
}
