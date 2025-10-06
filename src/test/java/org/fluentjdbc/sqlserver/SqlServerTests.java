package org.fluentjdbc.sqlserver;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import lombok.SneakyThrows;
import org.fluentjdbc.DbContextTableQueryBuilder;
import org.fluentjdbc.util.ExceptionUtil;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@RunWith(Enclosed.class)
public class SqlServerTests {

    private static final Map<String, String> REPLACEMENTS = new HashMap<>();

    static {
        REPLACEMENTS.put("UUID", "uniqueidentifier");
        REPLACEMENTS.put("INTEGER_PK", "integer identity primary key");
        REPLACEMENTS.put("BOOLEAN", "bit");
        REPLACEMENTS.put("BLOB", "varbinary(max)");
        REPLACEMENTS.put("CLOB", "varchar(max)");
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
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("set identity_insert demo_table on");
            }

            super.shouldInsertRowWithNonexistentKey();

            try (Statement stmt = connection.createStatement()) {
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
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("set identity_insert database_table_test_table on");
            }

            super.shouldInsertWithExplicitKey();

            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("set identity_insert database_table_test_table off");
            }
        }
    }

    public static class BulkInsertTest extends org.fluentjdbc.BulkInsertTest {
        public BulkInsertTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    public static class DbContextTest extends org.fluentjdbc.DbContextTest {
        public DbContextTest() {
            super(getDataSource(), REPLACEMENTS);
        }

        @SneakyThrows
        @Override
        public void shouldInsertOrUpdate() {
            try (Statement stmt = dbContext.getThreadConnection().createStatement()) {
                stmt.executeUpdate("set identity_insert database_table_test_table on");
            }
            super.shouldInsertOrUpdate();
        }

        @SneakyThrows
        @Override
        public void shouldInsertWithExplicitKey() {
            try (Statement stmt = dbContext.getThreadConnection().createStatement()) {
                stmt.executeUpdate("set identity_insert database_table_test_table on");
            }
            super.shouldInsertWithExplicitKey();
        }

        @Override
        public void shouldUpdateCalculatedFields() {
            // SQL Server syntax for string concatenation differs from other SQL dialects
        }

        @Override
        protected ByteArrayOutputStream readInputStream(DbContextTableQueryBuilder query, String column) {
            // Sql server closes streams when next() is called
            return query.singleObject(row -> toOutputStream(row.getInputStream(column))).get();
        }

        @Override
        protected String readFromReader(DbContextTableQueryBuilder query, String column) {
            // Sql server closes streams when next() is called
            return query.singleObject(row -> toString(row.getReader(column))).get();
        }
    }

    public static class DbContextSyncBuilderTest extends org.fluentjdbc.DbContextSyncBuilderTest {
        public DbContextSyncBuilderTest() {
            super(getDataSource(), REPLACEMENTS);
        }
    }

    public static class UsageDemonstrationTest extends org.fluentjdbc.usage.context.UsageDemonstrationTest {
        public UsageDemonstrationTest() {
            super(getDataSource(), REPLACEMENTS);
            databaseDoesNotSupportResultSetMetadataTableName();
        }
    }

    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private static boolean databaseFailed = false;

    private static SQLServerDataSource dataSource;

    static synchronized DataSource getDataSource() {
        Assume.assumeFalse("Database not available", databaseFailed);
        if (dataSource != null) {
            return dataSource;
        }
        dataSource = new SQLServerDataSource();
        dataSource.setLoginTimeout(2);
        dataSource.setURL(System.getProperty("test.db.sqlserver.url", "jdbc:sqlserver://localhost:1433;encrypt=false"));
        dataSource.setUser(System.getProperty("test.db.sqlserver.username", "sa"));
        dataSource.setPassword(System.getProperty("test.db.sqlserver.password", "0_A_SECRET_p455w0rd"));
        try {
            dataSource.getConnection().close();
        } catch (SQLException e) {
            if (e.getSQLState().equals("08S03") || e.getSQLState().equals("08S01")) {
                databaseFailed = true;
                Assume.assumeFalse("Database is unavailable: " + e, true);
            }
            throw ExceptionUtil.softenCheckedException(e);
        }
        return dataSource;
    }
}

