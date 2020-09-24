package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

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

    protected void createTable(Connection connection, String createTable) {
        createTable(connection, createTable, replacements);
    }

    public static void createTable(Connection connection, String createTable, Map<String, String> replacements) {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(preprocessCreateTable(createTable, replacements));
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public static String preprocessCreateTable(String createTableStatement, Map<String, String> replacements) {
        return createTableStatement
                .replaceAll(Pattern.quote("${UUID}"), replacements.get("UUID"))
                .replaceAll(Pattern.quote("${INTEGER_PK}"), replacements.get("INTEGER_PK"))
                .replaceAll(Pattern.quote("${DATETIME}"), replacements.get("DATETIME"))
                .replaceAll(Pattern.quote("${BOOLEAN}"), replacements.get("BOOLEAN"))
                .replaceAll(Pattern.quote("${INT_ARRAY}"), replacements.get("INT_ARRAY"))
                .replaceAll(Pattern.quote("${STRING_ARRAY}"), replacements.get("STRING_ARRAY"))
                ;
    }

    public static void dropTablesIfExists(Connection connection, String... tableNames) {
        Stream.of(tableNames).forEach(t -> dropTableIfExists(connection, t));
    }

    public static void dropTableIfExists(Connection connection, String tableName) {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table " + tableName);
        } catch(SQLException ignored) {
        }
    }

}
