package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseQueryBuilder extends DatabaseStatement implements DatabaseSimpleQueryBuilder, DatabaseListableQueryBuilder {

    private List<String> conditions = new ArrayList<>();
    private List<Object> parameters = new ArrayList<>();
    private List<String> orderByClauses = new ArrayList<>();
    private DatabaseTable table;

    DatabaseQueryBuilder(DatabaseTable table) {
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
            bindParameters(stmt, parameters);
            try (DatabaseResult result = new DatabaseResult(stmt)) {
                return result.list(mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"",
                    (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    @Override
    public List<Long> listLongs(Connection connection, final String fieldName) {
        return list(connection, new RowMapper<Long>() {
            @Override
            public Long mapRow(DatabaseRow row) throws SQLException {
                return row.getLong(fieldName);
            }
        });
    }

    @Override
    public List<String> listStrings(Connection connection, final String fieldName) {
        return list(connection, new RowMapper<String>() {
            @Override
            public String mapRow(DatabaseRow row) throws SQLException {
                return row.getString(fieldName);
            }
        });
    }


    @Nullable
    @Override
    public <T> T singleObject(Connection connection, RowMapper<T> mapper) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = new DatabaseResult(stmt)) {
                return result.single(mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"",
                    (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    @Override
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
    @Override
    public Number singleLong(Connection connection, final String fieldName) {
        return singleObject(connection, new RowMapper<Number>() {
            @Override
            public Number mapRow(DatabaseRow row) throws SQLException {
                return row.getLong(fieldName);
            }
        });
    }

    @Nullable
    @Override
    public Instant singleDateTime(Connection connection, final String fieldName) {
        return singleObject(connection, new RowMapper<Instant>() {
            @Override
            public Instant mapRow(DatabaseRow row) throws SQLException {
                return row.getInstant(fieldName);
            }
        });
    }

    private String createSelectStatement() {
        return "select * from " + table.getTableName()
                + (conditions.isEmpty() ? "" : " where " + join(" AND ", conditions))
                + (orderByClauses.isEmpty() ? "" : " order by " + join(", ", orderByClauses));
    }

    @Override
    public DatabaseSimpleQueryBuilder where(String fieldName, @Nullable Object value) {
        String expression = fieldName + " = ?";
        return whereExpression(expression, value);
    }

    @Override
    public DatabaseSimpleQueryBuilder whereOptional(String fieldName, @Nullable Object value) {
        if (value == null) return this;
        return where(fieldName, value);
    }

    public DatabaseSimpleQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        whereExpression(fieldName + " IN (" + join(",", repeat("?", parameters.size())) + ")");
        this.parameters.addAll(parameters);
        return this;
    }

    public DatabaseSimpleQueryBuilder whereExpression(String expression, @Nullable Object parameter) {
        whereExpression(expression);
        parameters.add(parameter);
        return this;
    }

    public DatabaseQueryBuilder whereExpression(String expression) {
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
