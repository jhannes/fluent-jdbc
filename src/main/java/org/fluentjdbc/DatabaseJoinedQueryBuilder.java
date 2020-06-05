package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DatabaseJoinedQueryBuilder extends DatabaseStatement implements DatabaseQueryBuilder<DatabaseJoinedQueryBuilder>, DatabaseListableQueryBuilder {
    private final DatabaseTableAlias table;
    private final List<JoinedTable> joinedTables = new ArrayList<>();
    private final List<String> conditions = new ArrayList<>();
    private final List<Object> parameters = new ArrayList<>();
    private final List<String> orderByClauses = new ArrayList<>();

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

    public DatabaseJoinedQueryBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters){
        whereExpression(expression);
        this.parameters.addAll(parameters);
        return this;
    }

    @Override
    public DatabaseJoinedQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        if (parameters.isEmpty()) {
            throw new IllegalArgumentException("Can't do " + fieldName + " IN (....) with empty list");
        }
        whereExpression(fieldName + " IN (" + parameterString(parameters.size()) + ")");
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

    @Override
    public DatabaseJoinedQueryBuilder whereAll(List<String> fields, List<Object> values) {
        fields.stream().map(s -> table.getAlias() + "." + s + " = ?").forEach(this.conditions::add);
        this.parameters.addAll(values);
        return this;
    }

    @Override
    public DatabaseJoinedQueryBuilder query() {
        return this;
    }

    public DatabaseJoinedQueryBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        joinedTables.add(new JoinedTable(a, b));
        return this;
    }

    protected String fromClause() {
        return " from " + table.getTableNameAndAlias() + " " +
                joinedTables.stream().map(JoinedTable::toSql).collect(Collectors.joining(" "));
    }

    @Nonnull
    @Override
    public <T> Optional<T> singleObject(Connection connection, DatabaseTable.RowMapper<T> mapper) {
        return query(connection, result -> result.single(mapper));
    }

    @Override
    public <T> List<T> list(Connection connection, DatabaseTable.RowMapper<T> mapper) {
        return query(connection, result -> result.list(mapper));
    }

    public void forEach(Connection connection, DatabaseTable.RowConsumer consumer) {
        query(connection, result -> {
            result.forEach(consumer);
            return null;
        });
    }

    private <T> T query(Connection connection, DatabaseResult.DatabaseResultMapper<T> resultMapper) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = createResult(stmt)) {
                return resultMapper.apply(result);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"", (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    @Override
    public int getCount(Connection connection) {
        long startTime = System.currentTimeMillis();
        String query = "select count(*) " + fromClause() + whereClause() + orderByClause();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Expected exactly one row returned from " + query);
                }
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"", (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    protected DatabaseResult createResult(PreparedStatement statement) throws SQLException {
        List<DatabaseTableAlias> aliases = new ArrayList<>();
        aliases.add(table);
        joinedTables.stream().map(JoinedTable::getAlias).forEach(aliases::add);

        Map<String, Map<String, Integer>> aliasColumnIndexes = new HashMap<>();
        aliases.forEach(t -> aliasColumnIndexes.put(t.getAlias().toUpperCase(), new HashMap<>()));
        int index = 0;


        ResultSet resultSet = statement.executeQuery();
        // Unfortunately, even though the database should know the alias for the each table, JDBC doesn't reveal it
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i=1; i<=metaData.getColumnCount(); i++) {
            while (!metaData.getTableName(i).equalsIgnoreCase(aliases.get(index).getTableName())) {
                index++;
                if (index == aliases.size()) {
                    throw new IllegalStateException("Failed to find table for column " + i + " (found " + aliasColumnIndexes + ") in " + aliases);
                }
            }
            String alias = aliases.get(index).getAlias().toUpperCase();
            String columnName = metaData.getColumnName(i).toUpperCase();
            if (aliasColumnIndexes.get(alias).containsKey(columnName)) {
                if (aliases.get(++index).getTableName().equalsIgnoreCase(metaData.getTableName(i))) {
                    alias = aliases.get(index).getAlias().toUpperCase();
                } else {
                    throw new IllegalStateException("Column twice in result " + alias + "." + columnName + ": " + aliasColumnIndexes);
                }
            }
            aliasColumnIndexes.get(alias).put(columnName, i);
        }

        return new DatabaseResult(statement, resultSet) {
            @Override
            public DatabaseRow row() {
                return new DatabaseRow(resultSet, columnIndexes, aliasColumnIndexes);
            }
        };
    }

    private String createSelectStatement() {
        return "select *" + fromClause() + whereClause() + orderByClause();
    }

    private String orderByClause() {
        return orderByClauses.isEmpty() ? "" : " order by " + String.join(", ", orderByClauses);
    }

    private String whereClause() {
        return conditions.isEmpty() ? "" : " where " + String.join(" AND ", conditions);
    }

    private static class JoinedTable {
        private final DatabaseColumnReference a;
        private final DatabaseColumnReference b;

        public JoinedTable(DatabaseColumnReference a, DatabaseColumnReference b) {
            this.a = a;
            this.b = b;
        }

        public String toSql() {
            return "inner join " + b.getTableNameAndAlias() + " on " + a.getQualifiedColumnName() + " = " + b.getQualifiedColumnName();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + toSql() + "]";
        }

        DatabaseTableAlias getAlias() {
            return b.getTableAlias();
        }
    }
}
