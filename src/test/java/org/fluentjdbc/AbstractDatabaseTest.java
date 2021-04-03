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

    public static String getDatabaseProductName(Connection connection) {
        try {
            return connection.getMetaData().getDatabaseProductName();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }


    public static String preprocessCreateTable(String createTableStatement, Map<String, String> replacements) {
        return createTableStatement
                .replaceAll(Pattern.quote("${UUID}"), replacements.getOrDefault("UUID", "uuid"))
                .replaceAll(Pattern.quote("${INTEGER_PK}"), replacements.getOrDefault("INTEGER_PK", "serial primary key"))
                .replaceAll(Pattern.quote("${DATETIME}"), replacements.getOrDefault("DATETIME", "datetime"))
                .replaceAll(Pattern.quote("${BOOLEAN}"), replacements.getOrDefault("BOOLEAN", "boolean"))
                .replaceAll(Pattern.quote("${INT_ARRAY}"), replacements.getOrDefault("INT_ARRAY", "integer array"))
                .replaceAll(Pattern.quote("${STRING_ARRAY}"), replacements.getOrDefault("STRING_ARRAY", "varchar(256) array"))
                .replaceAll(Pattern.quote("${BLOB}"), replacements.getOrDefault("BLOB", "blob"))
                .replaceAll(Pattern.quote("${CLOB}"), replacements.getOrDefault("CLOB", "clob"))
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
