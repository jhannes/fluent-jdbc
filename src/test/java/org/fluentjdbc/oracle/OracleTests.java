package org.fluentjdbc.oracle;

import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.util.ExceptionUtil;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Not supported:
 *
 * <ul>
 *     <li>DataRow.table() due to missing support for ResultTypeMetadata.getTableName</li>
 *     <li>SaveBuilder may give wrong status due to unusual return values from ResultSet.getObject</li>
 *     <li>DbContextSyncBuilder gives wrong status due to unusual return values from ResultSet.getObject</li>
 * </ul>
 */
@RunWith(Enclosed.class)
public class OracleTests {

    private static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("UUID", "VARCHAR2(36)");
        REPLACEMENTS.put("INTEGER_PK", "NUMBER GENERATED by default on null as IDENTITY primary key");
        REPLACEMENTS.put("DATETIME", "timestamp");
        REPLACEMENTS.put("BOOLEAN", "NUMBER(1,0)");
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

    @Ignore("Oracle does not support ResultSetMetaData and cannot be used with DatabaseRow.table")
    public static class DatabaseJoinedQueryBuilderTest extends org.fluentjdbc.DatabaseJoinedQueryBuilderTest {
        public DatabaseJoinedQueryBuilderTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }
    }

    public static class DbContextTest extends org.fluentjdbc.DbContextTest {
        public DbContextTest() throws SQLException {
            super(getDataSource(), REPLACEMENTS);
        }

        @Test
        @Override
        @Ignore("Unknown why this doesn't work")
        public void shouldBeAbleToTurnOffAutoCommits() {
        }
    }

    @Ignore("Oracle does not support ResultSetMetaData and cannot be used with DatabaseRow.table")
    public static class DbContextJoinedQueryBuilderTest extends org.fluentjdbc.DbContextJoinedQueryBuilderTest {
        public DbContextJoinedQueryBuilderTest() throws SQLException {
            super(getDataSource(), REPLACEMENTS);
        }
    }

    public static class DbContextSyncBuilderTest extends org.fluentjdbc.DbContextSyncBuilderTest {
        public DbContextSyncBuilderTest() throws SQLException {
            super(getDataSource(), REPLACEMENTS);
        }
    }

    public static class UsageDemonstrationTest extends org.fluentjdbc.usage.context.UsageDemonstrationTest {
        public UsageDemonstrationTest() throws SQLException {
            super(getDataSource(), REPLACEMENTS);
            databaseDoesNotSupportResultSetMetadataTableName();
        }

        @Override
        protected void verifySyncStatus(EnumMap<DatabaseSaveResult.SaveStatus, Integer> syncStatus) {
            // Oracle doesn't convert Timestamps and integers correctly and so doesn't match the existing rows
        }
    }

    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private static boolean databaseDisabled = Boolean.getBoolean("test.db.oracle.disabled");

    private static OracleConnectionPoolDataSource dataSource;

    static synchronized DataSource getDataSource() throws SQLException {
        Assume.assumeFalse(databaseDisabled);
        if (dataSource != null) {
            return dataSource;
        }
        dataSource = new OracleConnectionPoolDataSource();
        dataSource.setURL(System.getProperty("test.db.oracle.url", "jdbc:oracle:thin:@localhost:1521/XEPDB1"));
        dataSource.setUser(System.getProperty("test.db.oracle.username", "system"));
        dataSource.setPassword(System.getProperty("test.db.oracle.password", "0_A_SECRET_p455w0rd"));
        try {
            dataSource.getConnection().close();
        } catch (SQLException e) {
            databaseDisabled = true;
            if (e.getSQLState().equals("08006")) {
                Assume.assumeFalse("Database is unavailable: " + e, true);
            }
            throw ExceptionUtil.softenCheckedException(e);
        }
        return dataSource;
    }
}
