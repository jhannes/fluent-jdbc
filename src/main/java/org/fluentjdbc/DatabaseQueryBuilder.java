package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZonedDateTime;
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
    public String singleString(Connection connection, final String fieldName) {
        return singleObject(connection, new RowMapper<String>() {
            @Override
            public String mapRow(DatabaseRow row) throws SQLException {
                return row.getString(fieldName);
            }
        });
    }

    @Nullable
    public Number singleLong(Connection connection, final String fieldName) {
        return singleObject(connection, new RowMapper<Number>() {
            @Override
            public Number mapRow(DatabaseRow row) throws SQLException {
                return row.getLong(fieldName);
            }
        });
    }

    @Nullable
    public ZonedDateTime singleDateTime(Connection connection, final String fieldName) {
        return singleObject(connection, new RowMapper<ZonedDateTime>() {
            @Override
            public ZonedDateTime mapRow(DatabaseRow row) throws SQLException {
                return row.getDateTime(fieldName);
            }
        });
    }

    private String createSelectStatement() {
        return "select * from " + table.getTableName() +
                (conditions.isEmpty() ? "" : " where " + String.join(" AND ", conditions));
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

    public DatabaseUpdateBuilder update() {
        return table.update().setWhereFields(conditions, parameters);
    }



}
