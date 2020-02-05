package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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
        try {
            long startTime = System.currentTimeMillis();
            String query = createSelectStatement();
            logger.trace(query);
            PreparedStatement stmt = connection.prepareStatement(query);
            bindParameters(stmt);
            ResultSet rs = stmt.executeQuery();

            Iterator<T> iterator = new Iterator<T>() {
                private boolean hasNext;
                {
                    hasNext = rs.next();
                }

                @Override
                public boolean hasNext() {
                    return hasNext;
                }

                @Override
                public T next() {
                    try {
                        T o = mapper.mapRow(new DatabaseRow(rs));
                        hasNext = rs.next();
                        if (!hasNext) {
                            logger.debug("time={}s query=\"{}\"", (System.currentTimeMillis()-startTime)/1000.0, query);
                            close();
                        }
                        return o;
                    } catch (SQLException e) {
                        throw ExceptionUtil.softenCheckedException(e);
                    }
                }

                protected void close() throws SQLException {
                    rs.close();
                    stmt.close();
                }

                @Override
                protected void finalize() throws Throwable {
                    close();
                }
            };
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    @Override
    public <T> List<T> list(Connection connection, RowMapper<T> mapper) {
        return stream(connection, mapper).collect(Collectors.toList());
    }

    public void forEach(Connection connection, DatabaseTable.RowConsumer consumer) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt);
            try (DatabaseResult result = new DatabaseResult(stmt.executeQuery())) {
                result.forEach(consumer);
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
                + (conditions.isEmpty() ? "" : " where " + String.join(" AND ", conditions))
                + (orderByClauses.isEmpty() ? "" : " order by " + String.join(", ", orderByClauses));
    }

    protected String fromClause() {
        return " from " + table.getTableName();
    }

    @Override
    public DatabaseSimpleQueryBuilder whereOptional(String fieldName, @Nullable Object value) {
        if (value == null) return this;
        return where(fieldName, value);
    }

    public DatabaseSimpleQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        whereExpression(fieldName + " IN (" + parameterString(parameters.size()) + ")");
        this.parameters.addAll(parameters);
        return this;
    }

    public DatabaseSimpleQueryBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters) {
        whereExpression(expression);
        this.parameters.addAll(parameters);
        return this;
    }

    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression, @Nullable Object parameter) {
        whereExpression(expression);
        parameters.add(parameter);
        return this;
    }

    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression) {
        conditions.add(expression);
        return this;
    }

    public DatabaseSimpleQueryBuilder whereAll(List<String> fields, List<Object> values) {
        for (int i = 0; i < fields.size(); i++) {
            where(fields.get(i), values.get(i));
        }
        return this;
    }

    @Override
    public DatabaseUpdateBuilder update() {
        return table.update().setWhereFields(conditions, parameters);
    }

    @Override
    public DatabaseSimpleQueryBuilder query() {
        return this;
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
