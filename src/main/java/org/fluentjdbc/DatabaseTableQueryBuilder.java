package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseTableQueryBuilder extends DatabaseStatement implements DatabaseSimpleQueryBuilder, DatabaseListableQueryBuilder {

    private List<String> conditions = new ArrayList<>();
    private List<Object> parameters = new ArrayList<>();
    private List<String> orderByClauses = new ArrayList<>();
    private DatabaseTable table;

    DatabaseTableQueryBuilder(DatabaseTable table) {
        this.table = table;
    }

    public <T> Stream<T> stream(Connection connection, RowMapper<T> mapper) {
        return list(connection, mapper).stream();
    }

    @Override
    public <T> List<T> list(Connection connection, RowMapper<T> mapper) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt);
            try (DatabaseResult result = new DatabaseResult(stmt.executeQuery())) {
                return result.list(mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"",
                    (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    @Nullable
    @Override
    public <T> T singleObject(Connection connection, RowMapper<T> mapper) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt);
            try (DatabaseResult result = new DatabaseResult(stmt.executeQuery())) {
                return result.single(mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"",
                    (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    private int bindParameters(PreparedStatement stmt) throws SQLException {
        return bindParameters(stmt, parameters);
    }

    private String createSelectStatement() {
        return "select *" + fromClause()
                + (conditions.isEmpty() ? "" : " where " + join(" AND ", conditions))
                + (orderByClauses.isEmpty() ? "" : " order by " + join(", ", orderByClauses));
    }

    protected String fromClause() {
        return " from " + table.getTableName();
    }

    @Override
    public DatabaseSimpleQueryBuilder whereOptional(String fieldName, @Nullable Object value) {
        if (value == null) return this;
        return where(fieldName, value);
    }

    public DatabaseTableQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        whereExpression(fieldName + " IN (" + join(",", repeat("?", parameters.size())) + ")");
        this.parameters.addAll(parameters);
        return this;
    }

    public DatabaseTableQueryBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters) {
        whereExpression(expression);
        this.parameters.addAll(parameters);
        return this;
    }

    @Override
    public DatabaseTableQueryBuilder whereExpression(String expression, @Nullable Object parameter) {
        whereExpression(expression);
        parameters.add(parameter);
        return this;
    }

    @Override
    public DatabaseTableQueryBuilder whereExpression(String expression) {
        conditions.add(expression);
        return this;
    }

    public DatabaseSimpleQueryBuilder whereAll(List<String> fieldNames, List<Object> values) {
        for (int i = 0; i < fieldNames.size(); i++) {
            where(fieldNames.get(i), values.get(i));
        }
        return this;
    }

    @Override
    public DatabaseUpdateBuilder update() {
        return table.update().setWhereFields(conditions, parameters);
    }

    @Override
    public void delete(Connection connection) {
        table.delete().setWhereFields(conditions, parameters).execute(connection);
    }

    @Override
    public DatabaseListableQueryBuilder orderBy(String orderByClause) {
        orderByClauses.add(orderByClause);
        return this;
    }

    @Override
    public DatabaseListableQueryBuilder unordered() {
        return this;
    }

}
