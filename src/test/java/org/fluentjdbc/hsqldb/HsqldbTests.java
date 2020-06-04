package org.fluentjdbc.hsqldb;

import org.hsqldb.jdbc.JDBCDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public final class HsqldbTests {

    static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("UUID", "uuid");
        REPLACEMENTS.put("INTEGER_PK", "integer identity primary key");
        REPLACEMENTS.put("DATETIME", "datetime");
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

    public static class UsageDemonstrationTest extends org.fluentjdbc.usage.context.UsageDemonstrationTest {
        public UsageDemonstrationTest() {
            super(getDataSource(), REPLACEMENTS);
        }
    }

    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    public static DataSource getDataSource() {
        JDBCDataSource dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:test");
        dataSource.setUser("sa");
        return dataSource;
    }

}
