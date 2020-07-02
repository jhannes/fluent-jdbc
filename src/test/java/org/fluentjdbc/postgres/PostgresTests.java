package org.fluentjdbc.postgres;

import org.fluentjdbc.util.ExceptionUtil;
import org.junit.Assume;
import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class PostgresTests {

    private static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("UUID", "uuid");
        REPLACEMENTS.put("INTEGER_PK", "serial primary key");
        REPLACEMENTS.put("DATETIME", "timestamp");
        REPLACEMENTS.put("BOOLEAN", "boolean");
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

    public static class DbContextJoinedQueryBuilderTest extends org.fluentjdbc.DbContextJoinedQueryBuilderTest {
        public DbContextJoinedQueryBuilderTest() {
            super(getDataSource(), REPLACEMENTS);
        }
    }

    public static class DbSyncBuilderContextTest extends org.fluentjdbc.DbSyncBuilderContextTest {
        public DbSyncBuilderContextTest() {
            super(getDataSource(), REPLACEMENTS);
        }
    }

    public static class UsageDemonstrationTest extends org.fluentjdbc.usage.context.UsageDemonstrationTest {
        public UsageDemonstrationTest() {
            super(getDataSource(), REPLACEMENTS);
        }
    }

    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private static boolean databaseFailed = false;

    private static PGPoolingDataSource dataSource;

    static DataSource getDataSource() {
        Assume.assumeFalse(databaseFailed);
        if (dataSource != null) {
            return dataSource;
        }
        dataSource = new PGPoolingDataSource();
        String username = System.getProperty("test.db.postgres.username", "fluentjdbc_test");
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
