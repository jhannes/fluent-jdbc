package org.fluentjdbc.hsqldb;

import java.sql.Connection;
import java.sql.DriverManager;
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

    static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hsqldb:mem:test", "sa", null);
    }
}
