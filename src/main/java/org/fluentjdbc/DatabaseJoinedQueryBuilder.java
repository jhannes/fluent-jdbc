package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DatabaseJoinedQueryBuilder extends DatabaseStatement implements DatabaseQueryBuilder<DatabaseJoinedQueryBuilder>, DatabaseListableQueryBuilder {
    private final DatabaseTableAlias table;
    private List<JoinedTable> joinedTables = new ArrayList<>();
    private List<String> conditions = new ArrayList<>();
    private List<Object> parameters = new ArrayList<>();
    private List<String> orderByClauses = new ArrayList<>();

    public DatabaseJoinedQueryBuilder(DatabaseTableAlias table) {
        this.table = table;
    }

    @Override
    public DatabaseJoinedQueryBuilder unordered() {
        return this;
    }

    public DatabaseJoinedQueryBuilder orderBy(DatabaseColumnReference column) {
        return orderBy(column.getQualifiedColumnName());
    }

    @Override
    public DatabaseJoinedQueryBuilder orderBy(String orderByClause) {
        orderByClauses.add(orderByClause);
        return this;
    }

    @Override
    public DatabaseJoinedQueryBuilder where(String fieldName, @Nullable Object value) {
        return whereExpression(table.getAlias() + "." + fieldName + " = ?", value);
    }

    @Override
    public DatabaseJoinedQueryBuilder whereExpression(String expression, Object parameter) {
        whereExpression(expression);
        parameters.add(parameter);
        return this;
    }

    @Override
    public DatabaseJoinedQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        whereExpression(fieldName + " IN (" + join(",", repeat("?", parameters.size())) + ")");
        this.parameters.addAll(parameters);
        return this;
    }

    @Override
    public DatabaseJoinedQueryBuilder whereExpression(String expression) {
        conditions.add(expression);
        return this;
    }

    @Override
    public DatabaseJoinedQueryBuilder whereOptional(String fieldName, @Nullable Object value) {
        if (value == null) return this;
        return where(fieldName, value);
    }

    public DatabaseJoinedQueryBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        joinedTables.add(new JoinedTable(a, b));
        return this;
    }

    protected String fromClause() {
        return " from " + table.getTableNameAndAlias() + " " +
                joinedTables.stream().map(JoinedTable::toSql).collect(Collectors.joining(" "));
    }

    @Override
    public <T> T singleObject(Connection connection, DatabaseTable.RowMapper<T> mapper) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = createResult(stmt.executeQuery())) {
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
    public <T> List<T> list(Connection connection, DatabaseTable.RowMapper<T> mapper) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = createResult(stmt.executeQuery())) {
                return result.list(mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"",
                    (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    protected DatabaseResult createResult(ResultSet rs) throws SQLException {
        Map<DatabaseColumnReference, Integer> columnMap = new LinkedHashMap<>();
        List<DatabaseTableAlias> aliases = new ArrayList<>();
        aliases.add(table);
        joinedTables.stream().map(t -> t.getAlias()).forEach(aliases::add);
        int index = 0;

        // Unfortunately, even though the database should know the alias for the each table, JDBC doesn't reveal it
        ResultSetMetaData metaData = rs.getMetaData();
        for (int i=1; i<=metaData.getColumnCount(); i++) {
            while (!metaData.getTableName(i).equalsIgnoreCase(aliases.get(index).getTableName())) {
                index++;
                if (index == aliases.size()) {
                    throw new IllegalStateException("Failed to find table for column " + i + " (found " + columnMap + ")");
                }
            }
            DatabaseColumnReference column = aliases.get(index).column(metaData.getColumnName(i));
            if (columnMap.containsKey(column)) {
                if (aliases.get(++index).getTableName().equalsIgnoreCase(metaData.getTableName(i))) {
                    column = aliases.get(index).column(metaData.getColumnName(i));
                } else {
                    throw new IllegalStateException("Column twice in result " + column + ": " + columnMap);
                }
            }
            columnMap.put(column, i);
        }

        return new DatabaseResult(rs) {
            @Override
            protected DatabaseRow createDatabaseRow(ResultSet resultSet) throws SQLException {
                return new DatabaseRow(resultSet, columnMap);
            }
        };
    }

    private String createSelectStatement() {
        return "select *" + fromClause() + whereClause() + orderByClause();
    }

    private String orderByClause() {
        return orderByClauses.isEmpty() ? "" : " order by " + join(", ", orderByClauses);
    }

    private String whereClause() {
        return conditions.isEmpty() ? "" : " where " + join(" AND ", conditions);
    }

    private class JoinedTable {
        private final DatabaseColumnReference a;
        private final DatabaseColumnReference b;

        public JoinedTable(DatabaseColumnReference a, DatabaseColumnReference b) {
            this.a = a;
            this.b = b;
        }

        public String toSql() {
            return "inner join " + b.getTableAlias().getTableNameAndAlias() + " on " + a.getQualifiedColumnName() + " = " + b.getQualifiedColumnName();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + toSql() + "]";
        }

        public DatabaseTableAlias getAlias() {
            return b.getTableAlias();
        }
    }
}
