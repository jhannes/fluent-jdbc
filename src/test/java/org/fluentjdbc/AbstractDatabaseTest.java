package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class AbstractDatabaseTest {

    private final Map<String, String> replacements;

    public AbstractDatabaseTest(Map<String,String> replacements) {
        this.replacements = replacements;
    }

    protected String preprocessCreateTable(String createTableStatement) {
        return createTableStatement
                .replaceAll(Pattern.quote("${UUID}"), replacements.get("UUID"))
                .replaceAll(Pattern.quote("${INTEGER_PK}"), replacements.get("INTEGER_PK"))
                .replaceAll(Pattern.quote("${DATETIME}"), replacements.get("DATETIME"))
                .replaceAll(Pattern.quote("${BOOLEAN}"), replacements.get("BOOLEAN"))
                ;
    }

    protected void dropTablesIfExists(Connection connection, String... tableNames) {

        Stream.of(tableNames).forEach(t -> dropTableIfExists(connection, t));
    }

    protected void dropTableIfExists(Connection connection, String tableName) {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table " + tableName);
        } catch(SQLException ignored) {
        }
    }

}
