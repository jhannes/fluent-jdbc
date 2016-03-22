package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseQueryBuilder extends DatabaseStatement {

    private List<String> conditions = new ArrayList<>();
    private List<Object> parameters = new ArrayList<>();
    private DatabaseTable table;

    DatabaseQueryBuilder(DatabaseTable table) {
        this.table = table;
    }

    public <T> List<T> list(Connection connection, RowMapper<T> mapper) {
        logger.debug(createSelectStatement());
        try(PreparedStatement stmt = connection.prepareStatement(createSelectStatement())) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = new DatabaseResult(stmt)) {
                return result.list(table.getTableName(), mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    @Nullable
    public <T> T singleObject(Connection connection, RowMapper<T> mapper) {
        logger.debug(createSelectStatement());
        try(PreparedStatement stmt = connection.prepareStatement(createSelectStatement())) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = new DatabaseResult(stmt)) {
                return result.single(table.getTableName(), mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    @Nullable
    public String singleString(Connection connection, String fieldName) {
        return singleObject(connection, row -> row.getString(fieldName));
    }

    @Nullable
    public Number singleLong(Connection connection, String fieldName) {
        return singleObject(connection, row -> row.getLong(fieldName));
    }

    private String createSelectStatement() {
        return "select * from " + table.getTableName() + " where " + String.join(" AND ", conditions);
    }

    public DatabaseQueryBuilder where(String fieldName, @Nullable Object value) {
        String expression = fieldName + " = ?";
        return whereExpression(expression, value);
    }

    public DatabaseQueryBuilder whereExpression(String expression, @Nullable Object parameter) {
        conditions.add(expression);
        parameters.add(parameter);
        return this;
    }

    public DatabaseQueryBuilder whereAll(List<String> fieldNames, List<Object> values) {
        for (int i = 0; i < fieldNames.size(); i++) {
            where(fieldNames.get(i), values.get(i));
        }
        return this;
    }

    public void update(Connection connection, List<String> columns, List<Object> parameters) {
        update().update(connection, columns, parameters);
    }

    public DatabaseUpdateBuilder update() {
        return new DatabaseUpdateBuilder(table, this.conditions, this.parameters);
    }

}
