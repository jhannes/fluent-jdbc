package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabaseQueryBuilder extends DatabaseStatement {

    private final String tableName;
    private List<String> conditions = new ArrayList<>();

    public DatabaseQueryBuilder(String tableName) {
        this.tableName = tableName;
    }

    public <T> List<T> list(Connection connection, RowMapper<T> mapper) {
        logger.debug(createSelectStatement());
        try(PreparedStatement stmt = connection.prepareStatement(createSelectStatement())) {
            bindParameters(stmt);
            try (DatabaseResult result = new DatabaseResult(stmt)) {
                return result.list(tableName, mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public <T> T singleObject(Connection connection, RowMapper<T> mapper) {
        logger.debug(createSelectStatement());
        try(PreparedStatement stmt = connection.prepareStatement(createSelectStatement())) {
            bindParameters(stmt);
            try (DatabaseResult result = new DatabaseResult(stmt)) {
                return result.single(tableName, mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public String singleString(Connection connection, String fieldName) {
        return singleObject(connection, row -> row.getString(fieldName));
    }

    private String createSelectStatement() {
        return "select * from " + tableName + " where " + String.join(" AND ", conditions);
    }

    public DatabaseQueryBuilder where(String fieldName, Object value) {
        String expression = fieldName + " = ?";
        return whereExpression(expression, value);
    }

    public DatabaseQueryBuilder whereExpression(String expression, Object parameter) {
        conditions.add(expression);
        parameters.add(parameter);
        return this;
    }

}
