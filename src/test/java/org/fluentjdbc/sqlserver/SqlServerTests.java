package org.fluentjdbc.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import org.junit.Assume;
import org.junit.Ignore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class SqlServerTests {

    private static final Map<String, String> REPLACEMENTS = new HashMap<>();

    static {
        REPLACEMENTS.put("UUID", "uniqueidentifier");
        REPLACEMENTS.put("INTEGER_PK", "integer identity primary key");
        REPLACEMENTS.put("DATETIME", "datetime");
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

    public static class UsageDemonstrationTest extends org.fluentjdbc.usage.context.UsageDemonstrationTest {
        public UsageDemonstrationTest() throws SQLException {
            super(getDataSource(), REPLACEMENTS);
            databaseDoesNotSupportResultSetMetadataTableName();
        }
    }

    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private static boolean databaseFailed = false;

    private static SQLServerDataSource dataSource;

    static synchronized DataSource getDataSource() throws SQLException {
        Assume.assumeFalse(databaseFailed);
        if (dataSource != null) {
            return dataSource;
        }
        dataSource = new SQLServerDataSource();
        String username = System.getProperty("test.db.sqlserver.username", "sa");
        dataSource.setLoginTimeout(2);
        dataSource.setURL(System.getProperty("test.db.sqlserver.url", "jdbc:sqlserver://localhost:1433"));
        dataSource.setUser(username);
        dataSource.setPassword(System.getProperty("test.db.sqlserver.password", username));
        try {
            dataSource.getConnection().close();
        } catch (SQLException e) {
            if (e.getSQLState().equals("08S03") || e.getSQLState().equals("08S01")) {
                databaseFailed = true;
                Assume.assumeFalse("Database is unavailable: " + e, true);
            }
            throw e;
        }
        return dataSource;
    }
}

