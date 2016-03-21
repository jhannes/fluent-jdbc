package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DatabaseSaveBuilder extends DatabaseStatement {

    private List<String> columns = new ArrayList<>();
    private final String tableName;
    private String idField;
    private Long id;

    public DatabaseSaveBuilder(String tableName, String idField, Long id) {
        this.tableName = tableName;
        this.idField = idField;
        this.id = id;
    }

    public DatabaseSaveBuilder uniqueKey(String fieldName, Object fieldValue) {
        columns.add(fieldName);
        parameters.add(fieldValue);
        return this;
    }

    public DatabaseSaveBuilder setField(String fieldName, Object fieldValue) {
        columns.add(fieldName);
        parameters.add(fieldValue);
        return this;
    }

    public long execute(Connection connection) {
        if (id == null) {
            return insert(connection);
        } else {
            return update(connection);
        }
    }

    private long update(Connection connection) {
        logger.debug(createUpdateStatement());
        try (PreparedStatement stmt = connection.prepareStatement(createUpdateStatement())) {
            int index = bindParameters(stmt);
            bindParameter(stmt, index++, id);

            stmt.executeUpdate();
            return id;
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    private long insert(Connection connection) {
        logger.debug(createInsertStatement());
        try (PreparedStatement stmt = connection.prepareStatement(createInsertStatement(), Statement.RETURN_GENERATED_KEYS)) {
            bindParameters(stmt);
            stmt.executeUpdate();

            return getGeneratedKey(stmt);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    private long getGeneratedKey(PreparedStatement stmt) throws SQLException {
        try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
            if (!generatedKeys.next()) {
                throw new RuntimeException("Uh oh");
            }
            return generatedKeys.getLong(1);
        }
    }

    private String createUpdateStatement() {
        return "update " + tableName + " set " + String.join(",", updates()) + " where " + idField + " = ?";
    }

    private List<String> updates() {
        ArrayList<String> result = new ArrayList<>();
        for (String column : columns) {
            result.add(column + " = ?");
        }
        return result;
    }

    private String createInsertStatement() {
        return "insert into " + tableName + " (" + String.join(",", columns) + ") values (" + String.join(",", repeat("?", columns.size())) + ")";
    }

    private List<String> repeat(String string, int size) {
        ArrayList<String> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(string);
        }
        return result;
    }

}
