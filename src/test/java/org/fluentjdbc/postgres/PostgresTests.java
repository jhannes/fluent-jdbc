package org.fluentjdbc.postgres;

import org.fluentjdbc.util.ExceptionUtil;
import org.junit.Assume;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@RunWith(Enclosed.class)
public class PostgresTests {

    private static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("DATETIME", "timestamp");
        REPLACEMENTS.put("INT_ARRAY", "int[]");
        REPLACEMENTS.put("STRING_ARRAY", "varchar[]");
        REPLACEMENTS.put("BLOB", "bytea");
        REPLACEMENTS.put("CLOB", "text");
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

    public static class DatabaseJoinedQueryBuilderTest extends org.fluentjdbc.DatabaseJoinedQueryBuilderTest {
        public DatabaseJoinedQueryBuilderTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    public static class DbContextTest extends org.fluentjdbc.DbContextTest {
        public DbContextTest() {
            super(getDataSource(), REPLACEMENTS);
        }
    }

    public static class DbContextJoinedQueryBuilderTest extends org.fluentjdbc.DbContextJoinedQueryBuilderTest {
        public DbContextJoinedQueryBuilderTest() {
            super(getDataSource(), REPLACEMENTS);
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
        }
    }

    public static class DatabaseTableWithArraysTest extends org.fluentjdbc.usage.context.DatabaseTableWithArraysTest {
        public DatabaseTableWithArraysTest() {
            super(getDataSource(), REPLACEMENTS);
            requiresTypedArrays();
        }
    }

    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private static boolean databaseFailed = false;

    private static PGSimpleDataSource dataSource;

    static DataSource getDataSource() {
        Assume.assumeFalse(databaseFailed);
        if (dataSource != null) {
            return dataSource;
        }
        dataSource = new PGSimpleDataSource();
        String username = System.getProperty("test.db.postgres.username", "postgres");
        dataSource.setUrl(System.getProperty("test.db.postgres.url", "jdbc:postgresql:" + username));
        dataSource.setUser(username);
        dataSource.setPassword(System.getProperty("test.db.postgres.password", username));
        try {
            dataSource.getConnection().close();
        } catch (PSQLException e) {
            if (e.getSQLState().equals(PSQLState.CONNECTION_UNABLE_TO_CONNECT.getState())) {
                databaseFailed = true;
                Assume.assumeNoException(e);
            }
            throw ExceptionUtil.softenCheckedException(e);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
        return dataSource;
    }
}
