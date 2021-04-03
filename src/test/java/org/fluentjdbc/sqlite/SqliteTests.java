package org.fluentjdbc.sqlite;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.fluentjdbc.DatabaseSaveResult;
import org.junit.Ignore;
import org.junit.Test;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;

public class SqliteTests {

    static final Map<String, String> REPLACEMENTS = new HashMap<>();
    static {
        REPLACEMENTS.put("INTEGER_PK", "integer primary key autoincrement");
    }

    public static class DatabaseSaveBuilderTest extends org.fluentjdbc.DatabaseSaveBuilderTest {
        public DatabaseSaveBuilderTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }

        @Override
        public void shouldNotUpdateUnchangedRow() {
            // Ignored because Sqlite doesn't have proper timestamps
        }
    }

    public static class RichDomainModelTest extends org.fluentjdbc.RichDomainModelTest {
        public RichDomainModelTest() throws SQLException {
            super(getConnection(), REPLACEMENTS);
        }

        @Override
        @Test
        public void shouldBulkInsert() {
            // Sqlite currently only returns the generated key for the first in a batch
            assertThatThrownBy(super::shouldBulkInsert)
                .isInstanceOf(IllegalStateException.class);
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

    public static class UsageDemonstrationTest extends org.fluentjdbc.usage.context.UsageDemonstrationTest {
        public UsageDemonstrationTest() {
            super(getDataSource(), REPLACEMENTS);
        }

        @Override
        protected void verifySyncStatus(EnumMap<DatabaseSaveResult.SaveStatus, Integer> syncStatus) {
            // SQL Lite doesn't convert Timestamps correctly and so doesn't match the existing rows
        }
    }

    public static class DbContextTest extends org.fluentjdbc.DbContextTest {
        public DbContextTest() {
            super(getDataSource(), REPLACEMENTS);
            limitNotSupported();
        }

        @Test
        @Override
        @Ignore("Not supported on SQLite")
        public void shouldSeparateConnectionPerDbContext() {
            super.shouldSeparateConnectionPerDbContext();
        }
    }

    public static class DbContextJoinedQueryBuilderTest extends org.fluentjdbc.DbContextJoinedQueryBuilderTest {
        public DbContextJoinedQueryBuilderTest() {
            super(getDataSource(), REPLACEMENTS);
            limitNotSupported();
        }
    }

    public static DataSource getDataSource() {
        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:target/test-db-sqlite");
        return dataSource;
    }

    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }
}
